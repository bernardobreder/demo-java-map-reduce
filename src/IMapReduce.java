import java.util.Collection;
import java.util.Map.Entry;

/**
 * Interface que implementa a opera��o de Map e Reduce
 * 
 * @author Tecgraf
 * @param <D> Dado
 * @param <K> Chave
 * @param <V> Valor
 */
public interface IMapReduce<D, K, V> {

  /**
   * A fun��o divide os dados de entrada <D> em uma chave <K> e valor <V>.
   * 
   * @param input
   * @return chave e valor.
   */
  public Entry<K, V> map(D input);

  /**
   * A fun��o recebe uma cole��o de chave e valores e reduz para uma cole��o de
   * chave e valor
   * 
   * @param mapped dados mapeado
   * @return cole��o de chave e valor
   */
  public Entry<K, V> reduce(Entry<K, Collection<V>> mapped);

  /**
   * Implementa o resultado da opera��o de map e reduce numa thread anonima
   * 
   * @param result resultado do processamento
   */
  public void done(Collection<Entry<K, V>> result);

}
