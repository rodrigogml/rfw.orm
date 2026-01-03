package br.eng.rodrigogml.rfw.orm.measureruler.daoconverter;

import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.logger.RFWLogger;
import br.eng.rodrigogml.rfw.kernel.measureruler.MeasureRuler;
import br.eng.rodrigogml.rfw.kernel.measureruler.impl.CustomMeasureUnitGeneric;
import br.eng.rodrigogml.rfw.kernel.measureruler.interfaces.MeasureUnit;
import br.eng.rodrigogml.rfw.kernel.measureruler.interfaces.MeasureUnit.MeasureDimension;
import br.eng.rodrigogml.rfw.orm.dao.interfaces.RFWDAOConverterInterface;

/**
 * Description: Implementa o conversor do DAO para que seja possível persistir um objeto que utilize a MeasureUnit como um de seus atributos.<br>
 *
 * @author Rodrigo Leitão
 * @since 7.1.0 (13 de out de 2018)
 */
public class MeasureUnitDAOConverter implements RFWDAOConverterInterface {

  @Override
  public Object toVO(Object obj) {
    String value = (String) obj;
    MeasureUnit result = null;
    try {
      if (value != null) {
        if (value.startsWith("#")) {
          int index = value.indexOf('|');
          String symbol = value.substring(1, index);
          String name = value.substring(index + 1, value.length());
          result = new CustomMeasureUnitGeneric(name, symbol);
        } else {
          result = MeasureRuler.valueOf(value);
        }
      }
    } catch (RFWException e) {
      RFWLogger.logException(e);
      throw new RuntimeException("Erro ao converter MEASUREUNIT! " + value, e);
    }
    return result;
  }

  @Override
  public Object toDB(Object obj) {
    MeasureUnit value = (MeasureUnit) obj;
    String rvalue = null;
    if (value != null) {
      if (value.getDimension() == MeasureDimension.CUSTOM) {
        // Salva a unidade de medida custom começando com #, no padrão "#<Symbol>(<name>)", para que seja possível fazer o parser e recria-la quando for lida do banco de dados.
        rvalue = "#" + value.getSymbol() + "|" + value.name();
      } else {
        rvalue = value.name();
      }
    }
    return rvalue;
  }
}
