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
   * @return Nova entidade a ser mapeada em seu lugar, ou a mesma caso n�o seja necess�rio realizar nenhuma troca.
   */
  public default Class<? extends RFWVO> getEntityType(Class<? extends RFWVO> entityType) {
    return entityType;
  }

  /**
   * Este m�todo deve retornar o schema do banco de dados que deve ser utilizado com o VO.
   *
   * @param entityType entidade/RFWVO que se deseja saber o schema a ser utilizado.
   * @param entityDAOAnn {@link RFWDAO} Annotation atual da entidade.
   * @return String com o nome do Schema a ser utilizado no comando SQL.
   */
  public default String getSchema(Class<? extends RFWVO> entityType, RFWDAOAnnotation entityDAOAnn) throws RFWException {
    return null;
  }

  /**
   * Este m�todo deve retornar a tabela a ser utilizada pela entidade.
   *
   * @param entityType entidade/RFWVO que se deseja saber a tabela a ser utilizada.
   * @param entityDAOAnn {@link RFWDAO} Annotation atual da entidade.
   * @return String com o nome da tabela a ser utilizado no comando SQL.
   */
  public default String getTable(Class<? extends RFWVO> entityType, RFWDAOAnnotation entityDAOAnn) {
    return null;
  }

}
