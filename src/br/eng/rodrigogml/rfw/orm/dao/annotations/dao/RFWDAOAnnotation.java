package br.eng.rodrigogml.rfw.orm.dao.annotations.dao;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Description: Annotation usada para definir o catálogo a qual uma entidade pertence.<br>
 *
 * @author Rodrigo Leitão
 * @since 10.0.0 (12 de jul de 2018)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RFWDAOAnnotation {

  /**
   * Define o noma da tabela à qual esta entidade deve ser associada.
   */
  String table();

  /**
   * Permite definir o Schema a ser utilizado.Utilize as constantes de {@link RFWDAOAnnotation} {@link #SCHEMA_COMPANY} e {@link #SCHEMA_KERNEL}.<br>
   */
  String schema() default "";

}
