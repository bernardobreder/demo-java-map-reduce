import java.util.Map.Entry;

/**
 * 
 * 
 * @author Tecgraf
 * @param <K>
 * @param <V>
 */
public class MapReduceEntry<K extends Comparable<K>, V> implements Entry<K, V>,
  Comparable<K> {

  /** Chaves */
  protected final K key;
  /** Valor */
  protected final V value;

  /**
   * @param key
   * @param value
   */
  public MapReduceEntry(K key, V value) {
    super();
    this.key = key;
    this.value = value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public K getKey() {
    return key;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V getValue() {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V setValue(V value) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(K o) {
    return key.compareTo(o);
  }

  @Override
  public String toString() {
    return "<" + key + "," + value + ">";
  }

}
