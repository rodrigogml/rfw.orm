package br.eng.rodrigogml.rfw.orm.dao.interfaces;

/**
 * Description: Interface do conversor de dados, que deve ser anotado com a @RFWDAOConverterInterface para converter os dados que vão para o banco e/ou para o objeto.<br>
 *
 * @author Rodrigo Leitão
 * @since 7.1.0 (13 de out de 2018)
 */
public interface RFWDAOConverterInterface {

  /**
   * Converte o valor para ser colocado no Objeto.<br>
   * Note que o Objeto recebido será criado pelo Java, de acordo com o objeto padrão do Java para o tipo de coluna do bando de dados.
   *
   * @param value Valor como foi lido do banco de dados.
   * @return Objeto pronto para ser colocado no VO. Deve respeitar o tipo do objeto no VO
   */
  Object toVO(Object value);

  /**
   * Covnerte o valor para ser persistido na base de dados.<br>
   *
   * @param value Objeto que consta no VO
   * @return Valor para ser salvo no banco de dados.
   */
  Object toDB(Object value);

}
