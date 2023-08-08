package br.eng.rodrigogml.rfw.orm.dao.interfaces;

import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.vo.RFWVO;
import br.eng.rodrigogml.rfw.orm.dao.RFWDAO;
import br.eng.rodrigogml.rfw.orm.dao.annotations.dao.RFWDAOAnnotation;

/**
 * Description: Interface para permitir que algumas informa��es que precisam ser passadas para o RFWDAO sejam manipuladas/resolvidas pela aplica��o a medida que necess�rias pelo RFWDAO.<br>
 *
 * @author Rodrigo GML
 * @since 10.0 (12 de out de 2020)
 */
public interface DAOResolver {

  /**
   * Permite que o Resolver da aplica��o substitua uma entidade durante o mapeamento. Este m�todo � �til quando o RFW j� oferece alguns VOs de solu��es (como do Servi�o de Location), e a aplica��o tem objetod pr�prios descendentes dele. Este � um exemplo, � v�rios casos em que seja necess�rio a troca do objeto mapeado por outro descendente.
   *
   * @param entityType Entidade sendo mapeada pelo {@link RFWDAO}.
   * @return Nova entidade a ser mapeada em seu lugar, ou a mesma caso n�o seja necess�rio realizar nenhuma troca. Retornar nulo far� com que o RFWDAO siga sua implementa��o padr�o.
   */
  public default Class<? extends RFWVO> getEntityType(Class<? extends RFWVO> entityType) throws RFWException {
    return entityType;
  }

  /**
   * Este m�todo deve retornar o schema do banco de dados que deve ser utilizado com o VO.
   *
   * @param entityType entidade/RFWVO que se deseja saber o schema a ser utilizado.
   * @param entityDAOAnn {@link RFWDAO} Annotation atual da entidade.
   * @return String com o nome do Schema a ser utilizado no comando SQL. Retornar nulo far� com que o RFWDAO siga sua implementa��o padr�o.
   */
  public default String getSchema(Class<? extends RFWVO> entityType, RFWDAOAnnotation entityDAOAnn) throws RFWException {
    return null;
  }

  /**
   * Este m�todo deve retornar a tabela a ser utilizada pela entidade.
   *
   * @param entityType entidade/RFWVO que se deseja saber a tabela a ser utilizada.
   * @param entityDAOAnn {@link RFWDAO} Annotation atual da entidade.
   * @return String com o nome da tabela a ser utilizado no comando SQL. Retornar nulo far� com que o RFWDAO siga sua implementa��o padr�o.
   */
  public default String getTable(Class<? extends RFWVO> entityType, RFWDAOAnnotation entityDAOAnn) throws RFWException {
    return null;
  }

  /**
   * Este m�todo permite a customiza��o da instancia��o de novos objetos do sistema.<br>
   * Pode ser utilizao para objetos que requerem alguma inicializa��o diferenciada (como n�o ter um construtor sem argumentos), ou mesmo para trocar o objeto pela cria��o de uma classe descendente.<br>
   * Por exemplo, pode ser utilizada quando o sistema tiver suas pr�prias implementa��es das classes de Location ou outros VOs dos m�dulos oferecidos pelo RFW.
   *
   * @param objClass Classe do objeto que precisa ser instanciado.
   * @return Deve retornar a inst�ncia que possa ser auferida pela classe passada. Retornar nulo far� com que o RFWDAO siga sua implementa��o padr�o.
   */
  public default Object createInstance(Class<?> objClass) throws RFWException {
    return null;
  }

}
