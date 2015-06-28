/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include <mesos/executor.hpp>
#include <mesos/scheduler.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/http.hpp>
#include <process/pid.hpp>

#include <stout/base64.hpp>
#include <stout/hashmap.hpp>
#include <stout/option.hpp>

#include "master/flags.hpp"
#include "master/master.hpp"

#include "tests/mesos.hpp"
#include "tests/utils.hpp"

using std::string;
using std::vector;

using mesos::internal::master::Master;
using mesos::internal::slave::Slave;

using process::Future;
using process::PID;

using process::http::BadRequest;
using process::http::Conflict;
using process::http::OK;
using process::http::Response;
using process::http::Unauthorized;

using testing::_;
using testing::DoAll;
using testing::Eq;
using testing::SaveArg;
using testing::Return;

namespace mesos {
namespace internal {
namespace tests {


class ReservationEndpointsTest : public MesosTest {};


// TODO(mpark): Add tests for ACLs once they are introduced.


// This tests that an operator can reserve/unreserve available
// resources.
TEST_F(ReservationEndpointsTest, AvailableResources)
{
  TestAllocator<> allocator;

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = frameworkInfo.role();

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode(DEFAULT_CREDENTIAL.principal() + ":" +
                              DEFAULT_CREDENTIAL.secret());

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 512 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal(),
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal()).get();

  Future<Response> response = process::http::post(
      master.get(),
      "reserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  Future<vector<Offer>> offers;

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  EXPECT_CALL(sched, registered(&driver, _, _));

  EXPECT_CALL(sched, offerRescinded(_, _))
    .Times(0);

  driver.start();

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  Offer offer = offers.get()[0];

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources dynamicallyReserved = unreserved.flatten(
      frameworkInfo.role(),
      createReservationInfo(DEFAULT_CREDENTIAL.principal()));

  EXPECT_TRUE(Resources(offer.resources()).contains(dynamicallyReserved));

  // The filter to decline the offer "forever".
  Filters filtersForever;
  filtersForever.set_refuse_seconds(std::numeric_limits<double>::max());

  // Decline the offer "forever" in order to deallocate resources.
  driver.declineOffer(offer.id(), filtersForever);

  Future<Nothing> recoverResources;
  EXPECT_CALL(allocator, recoverResources(_, _, _, _))
    .WillOnce(DoAll(InvokeRecoverResources(&allocator),
                    FutureSatisfy(&recoverResources)));

  AWAIT_READY(recoverResources);

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  response = process::http::post(
      master.get(),
      "unreserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  offer = offers.get()[0];

  EXPECT_TRUE(Resources(offer.resources()).contains(unreserved));

  driver.stop();
  driver.join();

  Shutdown();
}


// This tests that an operator can reserve offered resources by
// rescinding the outstanding offers.
TEST_F(ReservationEndpointsTest, ReserveOfferedResources)
{
  TestAllocator<> allocator;

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = frameworkInfo.role();

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources dynamicallyReserved = unreserved.flatten(
      frameworkInfo.role(),
      createReservationInfo(DEFAULT_CREDENTIAL.principal()));

  Future<vector<Offer>> offers;

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  EXPECT_CALL(sched, registered(&driver, _, _));

  driver.start();

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  Offer offer = offers.get()[0];

  EXPECT_TRUE(Resources(offer.resources()).contains(unreserved));

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode(DEFAULT_CREDENTIAL.principal() + ":" +
                              DEFAULT_CREDENTIAL.secret());

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 512 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal(),
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal()).get();

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  // Expect an offer to be rescinded!
  EXPECT_CALL(sched, offerRescinded(_, _));

  Future<Response> response = process::http::post(
      master.get(),
      "reserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  offer = offers.get()[0];

  EXPECT_TRUE(Resources(offer.resources()).contains(dynamicallyReserved));

  driver.stop();
  driver.join();

  Shutdown();
}


// This tests that an operator can unreserve offered resources by
// rescinding the outstanding offers.
TEST_F(ReservationEndpointsTest, UnreserveOfferedResources)
{
  TestAllocator<> allocator;

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = frameworkInfo.role();

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode(DEFAULT_CREDENTIAL.principal() + ":" +
                              DEFAULT_CREDENTIAL.secret());

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 512 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal(),
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal()).get();

  Future<Response> response = process::http::post(
      master.get(),
      "reserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  Future<vector<Offer>> offers;

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  EXPECT_CALL(sched, registered(&driver, _, _));

  driver.start();

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  Offer offer = offers.get()[0];

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources dynamicallyReserved = unreserved.flatten(
      frameworkInfo.role(),
      createReservationInfo(DEFAULT_CREDENTIAL.principal()));

  EXPECT_TRUE(Resources(offer.resources()).contains(dynamicallyReserved));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  // Expect an offer to be rescinded!
  EXPECT_CALL(sched, offerRescinded(_, _));

  response = process::http::post(
      master.get(),
      "unreserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  offer = offers.get()[0];

  EXPECT_TRUE(Resources(offer.resources()).contains(unreserved));

  driver.stop();
  driver.join();

  Shutdown();
}


// This tests that an operator can reserve a mix of available and
// offered resources by rescinding the outstanding offers.
TEST_F(ReservationEndpointsTest, ReserveAvailableAndOfferedResources)
{
  TestAllocator<> allocator;

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = frameworkInfo.role();

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(&exec, slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources dynamicallyReserved = unreserved.flatten(
      frameworkInfo.role(),
      createReservationInfo(DEFAULT_CREDENTIAL.principal()));

  Future<vector<Offer>> offers;

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  EXPECT_CALL(sched, registered(&driver, _, _));

  driver.start();

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  Offer offer = offers.get()[0];

  EXPECT_TRUE(Resources(offer.resources()).contains(unreserved));

  Resources taskResources = Resources::parse("cpus:1;mem:256").get();

  // Create a task.
  TaskInfo taskInfo =
    createTask(offer.slave_id(), taskResources, "exit 1", exec.id);

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_FINISHED));

  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(DoDefault());

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  Filters filtersForever;
  filtersForever.set_refuse_seconds(std::numeric_limits<double>::max());

  driver.acceptOffers({offer.id()}, {LAUNCH({taskInfo})}, filtersForever);

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  offer = offers.get()[0];

  EXPECT_TRUE(Resources(offer.resources()).contains(taskResources));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode(DEFAULT_CREDENTIAL.principal() + ":" +
                              DEFAULT_CREDENTIAL.secret());

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 512 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal(),
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal()).get();

  // Expect an offer to be rescinded!
  EXPECT_CALL(sched, offerRescinded(_, _));

  Future<Response> response = process::http::post(
      master.get(),
      "reserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  offer = offers.get()[0];

  EXPECT_TRUE(Resources(offer.resources()).contains(dynamicallyReserved));

  driver.stop();
  driver.join();

  Shutdown();
}


// This tests that an operator can unreserve a mix of available and
// offered resources by rescinding the outstanding offers.
TEST_F(ReservationEndpointsTest, UnreserveAvailableAndOfferedResources)
{
  TestAllocator<> allocator;

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = frameworkInfo.role();

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(&exec, slaveFlags);
  ASSERT_SOME(slave);

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode(DEFAULT_CREDENTIAL.principal() + ":" +
                              DEFAULT_CREDENTIAL.secret());

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 512 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal(),
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal()).get();

  Future<Response> response = process::http::post(
      master.get(),
      "reserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  Future<vector<Offer>> offers;

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  EXPECT_CALL(sched, registered(&driver, _, _));

  driver.start();

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  Offer offer = offers.get()[0];

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources dynamicallyReserved = unreserved.flatten(
      frameworkInfo.role(),
      createReservationInfo(DEFAULT_CREDENTIAL.principal()));

  EXPECT_TRUE(Resources(offer.resources()).contains(dynamicallyReserved));

  Resources taskResources = Resources::parse("cpus:1;mem:256").get();
  taskResources = taskResources.flatten(
      frameworkInfo.role(),
      createReservationInfo(DEFAULT_CREDENTIAL.principal()));

  // Create a task.
  TaskInfo taskInfo =
    createTask(offer.slave_id(), taskResources, "exit 1", exec.id);

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_FINISHED));

  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(DoDefault());

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  Filters filtersForever;
  filtersForever.set_refuse_seconds(std::numeric_limits<double>::max());

  driver.acceptOffers({offer.id()}, {LAUNCH({taskInfo})}, filtersForever);

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  offer = offers.get()[0];

  EXPECT_TRUE(Resources(offer.resources()).contains(taskResources));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  // Expect an offer to be rescinded!
  EXPECT_CALL(sched, offerRescinded(_, _));

  response = process::http::post(
      master.get(),
      "unreserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers.get().size());
  offer = offers.get()[0];

  EXPECT_TRUE(Resources(offer.resources()).contains(unreserved));

  driver.stop();
  driver.join();

  Shutdown();
}


// This tests that an attempt to reserve/unreserve more resources
// than available results in a 'Conflict' HTTP error.
TEST_F(ReservationEndpointsTest, InsufficientResources)
{
  TestAllocator<> allocator;

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = frameworkInfo.role();

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode(DEFAULT_CREDENTIAL.principal() + ":" +
                              DEFAULT_CREDENTIAL.secret());

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 1024 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal(),
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal()).get();

  Future<Response> response = process::http::post(
      master.get(),
      "reserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(Conflict().status, response);

  response = process::http::post(
      master.get(),
      "unreserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(Conflict().status, response);

  Shutdown();
}


// This tests that an attempt to reserve with no authorization
// header results in a 'Unauthorized' HTTP error.
TEST_F(ReservationEndpointsTest, NoHeader)
{
  TestAllocator<> allocator;

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = frameworkInfo.role();

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 512 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal(),
      frameworkInfo.role(),
      DEFAULT_CREDENTIAL.principal()).get();

  Future<Response> response = process::http::post(
      master.get(),
      "reserve",
      None(),
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(
      Unauthorized("Mesos master").status,
      response);

  response = process::http::post(
      master.get(),
      "unreserve",
      None(),
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(
      Unauthorized("Mesos master").status,
      response);

  Shutdown();
}


// This tests that an attempt to reserve with bad credentials results
// in a 'Unauthorized' HTTP error.
TEST_F(ReservationEndpointsTest, BadCredentials)
{
  TestAllocator<> allocator;

  std::string role = "role";

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = role;

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode("badPrincipal:badSecret");

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 512 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      role,
      DEFAULT_CREDENTIAL.principal(),
      role,
      DEFAULT_CREDENTIAL.principal()).get();

  Future<Response> response = process::http::post(
      master.get(),
      "reserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(
      Unauthorized("Mesos master").status,
      response);

  response = process::http::post(
      master.get(),
      "unreserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(
      Unauthorized("Mesos master").status,
      response);

  Shutdown();
}


// This tests that an attempt to reserve with no 'slaveId' results in
// a 'BadRequest' HTTP error.
TEST_F(ReservationEndpointsTest, NoSlaveId)
{
  std::string role = "role";

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = role;

  Try<PID<Master>> master = StartMaster(masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Try<PID<Slave>> slave = StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode(DEFAULT_CREDENTIAL.principal() + ":" +
                              DEFAULT_CREDENTIAL.secret());

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 512 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      role,
      DEFAULT_CREDENTIAL.principal(),
      role,
      DEFAULT_CREDENTIAL.principal()).get();

  Future<Response> response = process::http::post(
      master.get(), "reserve", headers, "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  response = process::http::post(
      master.get(), "unreserve", headers, "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  Shutdown();
}


// This tests that an attempt to reserve with no 'resources' results
// in a 'BadRequest' HTTP error.
TEST_F(ReservationEndpointsTest, NoResources)
{
  TestAllocator<> allocator;

  std::string role = "role";

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = role;

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode(DEFAULT_CREDENTIAL.principal() + ":" +
                              DEFAULT_CREDENTIAL.secret());

  Future<Response> response = process::http::post(
      master.get(), "reserve", headers, "slaveId=" + slaveId.get().value());

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  response = process::http::post(
      master.get(), "unreserve", headers, "slaveId=" + slaveId.get().value());

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  Shutdown();
}


// This tests that an attempt to reserve with a non-matching
// principal results in a 'BadRequest' HTTP error.
TEST_F(ReservationEndpointsTest, NonMatchingPrincipal)
{
  TestAllocator<> allocator;

  std::string role = "role";

  master::Flags masterFlags = CreateMasterFlags();
  masterFlags.allocation_interval = Milliseconds(50);
  masterFlags.roles = role;

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<PID<Master>> master = StartMaster(&allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512";

  Future<SlaveID> slaveId;
  EXPECT_CALL(allocator, addSlave(_, _, _, _))
    .WillOnce(DoAll(InvokeAddSlave(&allocator),
                    FutureArg<0>(&slaveId)));

  Try<PID<Slave>> slave = StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  hashmap<string, string> headers;
  headers["Authorization"] =
    "Basic " + base64::encode(DEFAULT_CREDENTIAL.principal() + ":" +
                              DEFAULT_CREDENTIAL.secret());

  std::string resources = strings::format(
      R"~~([
        {
          "name" : "cpus",
          "type" : "SCALAR",
          "scalar" : { "value" : 1 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        },
        {
          "name" : "mem",
          "type" : "SCALAR",
          "scalar" : { "value" : 512 },
          "role" : "%s",
          "reservation" : {
            "principal" : "%s"
          }
        }
      ])~~",
      role,
      "badPrincipal",
      role,
      DEFAULT_CREDENTIAL.principal()).get();

  Future<Response> response = process::http::post(
      master.get(),
      "reserve",
      headers,
      "slaveId=" + slaveId.get().value() + "&" + "resources=" + resources);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);

  Shutdown();
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
