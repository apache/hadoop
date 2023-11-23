package org.apache.hadoop.fs.azurebfs.services;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class MetricPercentile {

  private long[] array; // circular array to store the elements

  private int head; // index of the first element

  private int tail; // index of the last element

  private int size; // number of elements in the queue

  private int capacity; // maximum capacity of the queue

  private LinkedList<Long> list; // sorted list to store the elements


  private static final Map<String, MetricPercentile> sendPercentileMap = new HashMap<>();
  private static final Map<String, MetricPercentile> rcvPercentileMap = new HashMap<>();
  private static final Map<String, MetricPercentile> totalPercentileMap = new HashMap<>();

  static {
    for(AbfsRestOperationType operationType : AbfsRestOperationType.values()) {
      sendPercentileMap.put(operationType.name(), new MetricPercentile(1000));
      rcvPercentileMap.put(operationType.name(), new MetricPercentile(1000));
      totalPercentileMap.put(operationType.name(), new MetricPercentile(1000));
    }
  }

  public static void addSendDataPoint(AbfsRestOperationType abfsRestOperationType, Long timeDuration) {
    sendPercentileMap.get(abfsRestOperationType.name()).push(timeDuration);
  }

  public static void addRcvDataPoint(AbfsRestOperationType abfsRestOperationType, Long timeDuration) {
    rcvPercentileMap.get(abfsRestOperationType.name()).push(timeDuration);
  }

  public static void addTotalDataPoint(AbfsRestOperationType abfsRestOperationType, Long timeDuration) {
    totalPercentileMap.get(abfsRestOperationType.name()).push(timeDuration);
  }

  public static Long getSendPercentileVal(AbfsRestOperationType abfsRestOperationType, Double percentile) {
    return sendPercentileMap.get(abfsRestOperationType.name()).percentile(percentile);
  }

  public static Long getRcvPercentileVal(AbfsRestOperationType abfsRestOperationType, Double percentile) {
    return rcvPercentileMap.get(abfsRestOperationType.name()).percentile(percentile);
  }

  public static Long getTotalPercentileVal(AbfsRestOperationType abfsRestOperationType, Double percentile) {
    return totalPercentileMap.get(abfsRestOperationType.name()).percentile(percentile);
  }



  // constructor
  public MetricPercentile(int capacity) {
    this.array = new long[capacity];
    this.head = 0;
    this.tail = -1;
    this.size = 0;
    this.capacity = capacity;
    this.list = new LinkedList<>();
  }

  // add an element to the queue
  public void push(long element) {
    // if the queue is full, remove the oldest element
    if (size == capacity) {
      pop();
    }
    // increment the tail index and wrap around if necessary
    tail = (tail + 1) % capacity;
    // store the element in the array
    array[tail] = element;
    // insert the element in the sorted list
    insert(element);
    // increment the size
    size++;
  }

  // remove the oldest element from the queue
  public long pop() {
    // if the queue is empty, throw an exception
    if (size == 0) {
      throw new RuntimeException("Queue is empty");
    }
    // get the element from the array
    long element = array[head];
    // remove the element from the sorted list
    remove(element);
    // increment the head index and wrap around if necessary
    head = (head + 1) % capacity;
    // decrement the size
    size--;
    // return the element
    return element;
  }

  // get the size of the queue
  public int size() {
    return size;
  }

  // get the capacity of the queue
  public int capacity() {
    return capacity;
  }

  // get the percentile value of the queue
  public long percentile(double p) {
    // if the queue is empty, throw an exception
    if (size == 0) {
      return Long.MAX_VALUE;
    }
    // if the percentile is out of range, throw an exception
    if (p < 0 || p > 100) {
      throw new IllegalArgumentException(
          "Percentile must be between 0 and 100");
    }
    // calculate the index of the percentile element in the sorted list
    int index = (int) Math.ceil(p / 100.0 * size) - 1;
    // return the element at that index
    return list.get(index);
  }

  // insert an element in the sorted list
  private void insert(long element) {
    // use binary search to find the insertion position
    int low = 0;
    int high = list.size() - 1;
    while (low <= high) {
      int mid = (low + high) / 2;
      if (element < list.get(mid)) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    // insert the element at the position
    list.add(low, element);
  }

  // remove an element from the sorted list
  private void remove(long element) {
    // use binary search to find the element position
    int low = 0;
    int high = list.size() - 1;
    while (low <= high) {
      int mid = (low + high) / 2;
      if (element < list.get(mid)) {
        high = mid - 1;
      } else if (element > list.get(mid)) {
        low = mid + 1;
      } else {
        // remove the element at the position
        list.remove(mid);
        return;
      }
    }
    // if the element is not found, throw an exception
    throw new RuntimeException("Element not found");
  }
}