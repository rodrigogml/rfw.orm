package br.eng.rodrigogml.rfw.orm.utils;

import br.eng.rodrigogml.rfw.kernel.exceptions.RFWCriticalException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.vo.RFWVO;
import br.eng.rodrigogml.rfw.orm.dao.annotations.dao.RFWDAOAnnotation;

/**
 * Description: Classe com utilit�rios para tratar objetos apartir de reflex�o.<br>
 *
 * @author Rodrigo Leit�o
 * @since 3.0.0 (SET / 2009)
 */
public class RUDAO {

  /**
   * Construtor privado para classe utilit�ria com m�todos exclusivamente est�ticos.
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
      throw new RFWCriticalException("A entidade '${0}' n�o possui a Annotation RFWDAO, e n�o pode ser interpretada.", new String[] { type.getCanonicalName() });
    }
    return ann;
  }
}
