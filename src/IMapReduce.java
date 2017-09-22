import java.util.Collection;
import java.util.Map.Entry;

/**
 * Interface que implementa a operação de Map e Reduce
 * 
 * @author Tecgraf
 * @param <D> Dado
 * @param <K> Chave
 * @param <V> Valor
 */
public interface IMapReduce<D, K, V> {

  /**
   * A função divide os dados de entrada <D> em uma chave <K> e valor <V>.
   * 
   * @param input
   * @return chave e valor.
   */
  public Entry<K, V> map(D input);

  /**
   * A função recebe uma coleção de chave e valores e reduz para uma coleção de
   * chave e valor
   * 
   * @param mapped dados mapeado
   * @return coleção de chave e valor
   */
  public Entry<K, V> reduce(Entry<K, Collection<V>> mapped);

  /**
   * Implementa o resultado da operação de map e reduce numa thread anonima
   * 
   * @param result resultado do processamento
   */
  public void done(Collection<Entry<K, V>> result);

}
