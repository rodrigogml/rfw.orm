package br.eng.rodrigogml.rfw.orm.dao.annotations.dao;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import br.eng.rodrigogml.rfw.orm.dao.interfaces.RFWDAOConverterInterface;

/**
 * Description: Esta Annotation permite definir um "interprete" para converter os dados que est�o no VO e o que ser� escrito na base de dados.<br>
 * Essa ferramenta � muito �til quando temos algum valor que precisa ser tratado antes de ir para o banco, e vice-versa. Como valores que no Objeto est�o como uma interface e o RFWDAO n�o sabe que objeto instanciar.
 *
 * @author Rodrigo Leit�o
 * @since 7.1.0 (13 de out de 2018)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface RFWDAOConverter {

  /**
   * Define a classe do {@link RFWDAOConverterInterface} que ser� utilizado.
   */
  Class<?> converterClass();

}
