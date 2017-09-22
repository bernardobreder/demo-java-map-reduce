import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * 
 * @author Tecgraf
 * @param <D>
 * @param <K>
 * @param <V>
 */
public class MapReduceEngine<D, K extends Comparable<K>, V> {

  /** Implementação da operação de Map e Reduce */
  private final IMapReduce<D, K, V> mapReduce;
  /** Thread o Master */
  private Thread masterThread;
  /** Thread o Master */
  private Thread[] workerThread = new Thread[2];
  /** Entrada de dados */
  private Collection<D> input;
  /** Input para cada Slave */
  private Map<Thread, D> inputMap;
  /** Input para cada Slave */
  private Map<Thread, Entry<K, Collection<V>>> inputReduce;
  /** Resultado Mapeado */
  private List<Entry<K, V>> mappedList;
  /** Lista de itens a reduzir */
  private List<Entry<K, Collection<V>>> groupList;
  /** Resultado Mapeado */
  private Collection<Entry<K, V>> resultList;
  /** Estado */
  private final AtomicInteger stage;

  /**
   * Construtor
   * 
   * @param mapReduce
   */
  public MapReduceEngine(IMapReduce<D, K, V> mapReduce) {
    this.mapReduce = mapReduce;
    this.stage = new AtomicInteger(0);
  }

  /**
   * Inicializa a engine do Map e Reduce
   * 
   * @param input
   */
  public void start(Collection<D> input) {
    this.input = input;
    this.masterThread = new Thread(new Runnable() {
      @Override
      public void run() {
        runMaster();
      }
    }, "Master");
    for (int n = 0; n < workerThread.length; n++) {
      workerThread[n] = new Thread(new Runnable() {
        @Override
        public void run() {
          runSlave();
        }
      }, "Slave-" + (n + 1));
      workerThread[n].start();
    }
    this.masterThread.start();
  }

  /**
   * Execução do Master
   */
  public void runMaster() {
    {
      inputMap = new Hashtable<Thread, D>();
      mappedList = new ArrayList<Map.Entry<K, V>>(input.size());
      stage.set(1);
      wakeupAllWorkers();
      int threadIndex = 0;
      Iterator<D> iterator = input.iterator();
      while (iterator.hasNext()) {
        D next = iterator.next();
        boolean found = false;
        while (!found) {
          for (int n = 0; n < workerThread.length; n++) {
            int i = ((threadIndex++) + n) % workerThread.length;
            Thread thread = workerThread[i];
            D inputItem = inputMap.get(thread);
            if (inputItem == null) {
              found = true;
              inputMap.put(thread, next);
              synchronized (thread) {
                thread.notify();
              }
              break;
            }
          }
          if (!found) {
            sleepUntilNotify(masterThread);
          }
        }
      }
      while (!inputMap.isEmpty()) {
        sleepUntilNotify(masterThread);
      }
    }
    {
      stage.set(2);
      Collections.sort(mappedList, new Comparator<Entry<K, V>>() {
        @Override
        public int compare(Entry<K, V> o1, Entry<K, V> o2) {
          return o1.getKey().compareTo(o2.getKey());
        }
      });
      groupList = new ArrayList<Entry<K, Collection<V>>>(mappedList.size());
      for (Entry<K, V> entry : mappedList) {
        Entry<K, Collection<V>> last =
          groupList.isEmpty() ? null : groupList.get(groupList.size() - 1);
        if (last == null || !last.getKey().equals(entry.getKey())) {
          ArrayList<V> values = new ArrayList<V>();
          values.add(entry.getValue());
          MapReduceEntry<K, Collection<V>> item =
            new MapReduceEntry<K, Collection<V>>(entry.getKey(), values);
          groupList.add(item);
        }
        else {
          last.getValue().add(entry.getValue());
        }
      }
      mappedList.clear();
    }
    {
      inputReduce = new Hashtable<Thread, Entry<K, Collection<V>>>();
      resultList = new ArrayList<Map.Entry<K, V>>(groupList.size());
      stage.set(3);
      wakeupAllWorkers();
      int threadIndex = 0;
      Iterator<Entry<K, Collection<V>>> iterator = groupList.iterator();
      while (iterator.hasNext()) {
        Entry<K, Collection<V>> next = iterator.next();
        boolean found = false;
        while (!found) {
          for (int n = 0; n < workerThread.length; n++) {
            int i = ((threadIndex++) + n) % workerThread.length;
            Thread thread = workerThread[i];
            Entry<K, Collection<V>> inputItem = inputReduce.get(thread);
            if (inputItem == null) {
              found = true;
              inputReduce.put(thread, next);
              synchronized (thread) {
                thread.notify();
              }
              break;
            }
          }
          if (!found) {
            synchronized (masterThread) {
              try {
                masterThread.wait();
              }
              catch (InterruptedException e) {
              }
            }
          }
        }
      }
      while (!inputReduce.isEmpty()) {
        sleepUntilNotify(masterThread);
      }
    }
    {
      stage.set(4);
      wakeupAllWorkers();
      mapReduce.done(resultList);
    }
  }

  /**
   * Execução do Slave
   */
  public void runSlave() {
    Thread thread = Thread.currentThread();
    while (stage.intValue() < 4) {
      int stageValue = stage.intValue();
      if (stageValue == 1) {
        D input = inputMap.get(thread);
        if (input != null) {
          Entry<K, V> mapped = mapReduce.map(input);
          synchronized (mappedList) {
            mappedList.add(mapped);
          }
          inputMap.remove(thread);
          synchronized (masterThread) {
            masterThread.notify();
          }
          continue;
        }
      }
      else if (stageValue == 3) {
        Entry<K, Collection<V>> input = inputReduce.get(thread);
        if (input != null) {
          Entry<K, V> result = mapReduce.reduce(input);
          synchronized (resultList) {
            resultList.add(result);
          }
          inputReduce.remove(thread);
          synchronized (masterThread) {
            masterThread.notify();
          }
          continue;
        }
      }
      sleepUntilNotify(thread);
    }
  }

  protected void wakeupAllWorkers() {
    for (int n = 0; n < workerThread.length; n++) {
      Thread thread = workerThread[n];
      synchronized (thread) {
        thread.notify();
      }
    }
  }

  protected static void sleepUntilNotify(Thread thread) {
    synchronized (thread) {
      try {
        thread.wait();
      }
      catch (InterruptedException e) {
      }
    }
  }
}
