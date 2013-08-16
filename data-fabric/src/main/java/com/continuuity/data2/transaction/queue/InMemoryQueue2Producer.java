package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Producer for an in-memory queue.
 */
public class InMemoryQueue2Producer implements Queue2Producer, TransactionAware {

  final InMemoryQueue queue;
  final List<QueueEntry> toEnqueue = Lists.newArrayList();
  Transaction currentTx = null;
  boolean committed = false;

  public InMemoryQueue2Producer(QueueName queueName) {
    this.queue = InMemoryQueueService.getQueue(queueName);
  }

  @Override
  public void enqueue(QueueEntry entry) throws IOException {
    if (committed) {
      throw new RuntimeException("enqueue called after commit. ");
    }
    toEnqueue.add(entry);
  }

  @Override
  public void enqueue(Iterable<QueueEntry> entries) throws IOException {
    if (committed) {
      throw new RuntimeException("enqueue called after commit. ");
    }
    for (QueueEntry entry : entries) {
      toEnqueue.add(entry);
    }
  }

  @Override
  public void startTx(Transaction tx) {
    currentTx = tx;
    toEnqueue.clear();
    committed = false;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return null; // enqueue cannot generate conflicts
  }

  @Override
  public boolean commitTx() throws Exception {
    if (committed) {
      throw new RuntimeException("commit called again after commit. ");
    }
    int seqId = 0;
    for (QueueEntry entry : toEnqueue) {
      queue.enqueue(currentTx.getWritePointer(), seqId++, entry);
    }
    committed = true;
    return true;
  }

  @Override
  public void postTxCommit() {
    toEnqueue.clear(); // get rid of the references to committed queue entries
  }

  @Override
  public boolean rollbackTx() throws Exception {
    if (committed) {
      for (int seqId = 0; seqId < toEnqueue.size(); seqId++) {
        queue.undoEnqueue(currentTx.getWritePointer(), seqId);
      }
    }
    return true;
  }
}
