import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

/**
 * 
 * 
 * @author Tecgraf
 */
public class Main implements IMapReduce<String, Integer, Integer> {

  /**
   * @param args
   */
  public static void main(String[] args) {
    MapReduceEngine<String, Integer, Integer> engine =
      new MapReduceEngine<String, Integer, Integer>(new Main());
    engine.start(Arrays.asList("abc1985de01", "abc1986de05", "abc1986de04",
      "abc1987de02", "abc1987de04"));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Entry<Integer, Integer> map(String input) {
    Integer key = Integer.valueOf(input.substring(3, 7));
    Integer value = Integer.valueOf(input.substring(9));
    return new MapReduceEntry<Integer, Integer>(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Entry<Integer, Integer> reduce(
    Entry<Integer, Collection<Integer>> input) {
    int value = 0;
    for (Integer item : input.getValue()) {
      value += item;
    }
    return new MapReduceEntry<Integer, Integer>(input.getKey(), value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void done(Collection<Entry<Integer, Integer>> result) {
    for (Entry<Integer, Integer> entry : result) {
      System.out.println(entry);
    }
  }

}
