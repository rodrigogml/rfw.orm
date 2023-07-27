package br.eng.rodrigogml.rfw.orm.utils;

import br.eng.rodrigogml.rfw.kernel.exceptions.RFWCriticalException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.vo.RFWVO;
import br.eng.rodrigogml.rfw.orm.dao.annotations.dao.RFWDAOAnnotation;

/**
 * Description: Classe com utilitários para tratar objetos apartir de reflexão.<br>
 *
 * @author Rodrigo Leitão
 * @since 3.0.0 (SET / 2009)
 */
public class RUDAO {

  /**
   * Construtor privado para classe utilitária com métodos exclusivamente estáticos.
   */
  private RUDAO() {
  }

  /**
   * Obtem Annotation RFWDAO da entidade.
   *
   * @return Annotation RFWDAO da entidade
   */
  public static RFWDAOAnnotation getRFWDAOAnnotation(Class<? extends RFWVO> type) throws RFWException {
    final RFWDAOAnnotation ann = type.getAnnotation(RFWDAOAnnotation.class);
    if (ann == null) {
      throw new RFWCriticalException("A entidade '${0}' não possui a Annotation RFWDAO, e não pode ser interpretada.", new String[] { type.getCanonicalName() });
    }
    return ann;
  }
}
