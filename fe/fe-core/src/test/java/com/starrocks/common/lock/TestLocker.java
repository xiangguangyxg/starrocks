// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.starrocks.common.lock;

import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

public class TestLocker {
    private final BlockingQueue<LockTask> lockTaskBlockingQueue;
    private final LockThread lockThread;

    public TestLocker() {
        lockTaskBlockingQueue = new ArrayBlockingQueue<>(1);
        lockThread = new LockThread(lockTaskBlockingQueue);
        lockThread.start();
    }

    public Future<LockResult> lock(Long rid, LockType lockType) {
        return lock(rid, lockType, 0);
    }

    public Future<LockResult> lock(Long rid, LockType lockType, long timeout) {
        LockTask lockTask = new LockTask(LockTask.LockState.LOCK, lockThread);
        lockTask.rid = rid;
        lockTask.lockType = lockType;
        lockTask.timeout = timeout;

        try {
            lockTaskBlockingQueue.put(lockTask);
            return lockTask;
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }
    }

    public Future<LockResult> release(Long rid, LockType lockType) {
        LockTask lockTask = new LockTask(LockTask.LockState.RELEASE, lockThread);
        lockTask.rid = rid;
        lockTask.lockType = lockType;

        try {
            lockTaskBlockingQueue.put(lockTask);
            return lockTask;
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }
    }

    public Locker getLocker() {
        return lockThread.getLocker();
    }

    @Test
    public void testSetQueryId() {
        Locker locker = new Locker();
        locker.setQueryId(UUID.randomUUID());
        Assertions.assertNotNull(locker.getQueryId());
    }
}
