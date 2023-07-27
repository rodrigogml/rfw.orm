package br.eng.rodrigogml.rfw.orm.vo;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import br.eng.rodrigogml.rfw.orm.dao.RFWDAO;

/**
 * Description: Estrutura utilizada para criar "campos especiais" do select. Permitindo que sejam retornados não os valores dos campos, mas sim resultados de funções que normalmente os bancos de dados já suportam, por exemplo:<br>
 * <li>count(id)</li>
 * <li>sum(total)</li> <br>
 * <br>
 * Sendo assim possível buscar contadores e valores sumarizados diretamente, ou mesmo os valores de apenas algumas colunas, ao invés de um objeto completamente montado.
 *
 * @author Rodrigo GML
 * @since 10.0 (22 de nov de 2019)
 */
public class RFWField implements Serializable, Cloneable {

  private static final long serialVersionUID = 5486452995928963502L;

  public static enum FieldFunction {
    /**
     * Define que trata-se apenas de um camp, sem função encapsulando.
     */
    FIELD,
    /**
     * Permite colocar um valor constante no lugar do campo ou como parâmetro para outrao função.
     */
    CONSTANTE_STRING,
    /**
     * Permite colocar um valor constante no lugar do campo ou como parâmetro para outrao função.
     */
    CONSTANTE_NUMBER,
    /**
     * Permite colocar um valor constante NULL no lugar do campo ou como parâmetro para outrao função.
     */
    CONSTANT_NULL,
    /**
     * Define uma função de soma dos valores.
     */
    SUM,
    /**
     * Cria a função COUNT(*) para contabilizar o total de resultados retornados.
     */
    COUNT,
    /**
     * Cria a função COALESCE para recuperar valores que não estejam nulos.
     */
    COALESCE,
    /**
     * Cria a função MONTH para separar o mês de uma coluna de data.
     */
    MONTH,
    /**
     * Cria a função YEAR para separar o ano de uma coluna de data.
     */
    YEAR,
    /**
     * Cria a função de Mínimo de um valor.
     */
    MINIMUM,
    /**
     * Cria a função de Máximo de um valor.
     */
    MAXIMUM,
    /**
     * Cria a função aritmética que permite multiplicar dois parâmetros.
     */
    MULTIPLY,
    /**
     * Cria a função aritmética que permite dividir dois parâmetros.
     */
    DIVIDE,
    /**
     * Cria a função concatenar. Quer permite juntar as partes de campos ou de constantes em uma única String/Coluna.
     */
    CONCAT,
  }

  /**
   * Nome da coluna, quando a função foi criada diretamente com um nome de coluna (e não como uma função aninhada).<br>
   * Permite que o {@link RFWDAO} consiga saber que o valor é um caminho de coluna (utilizando a estrutura do Match Objetc) para saber como conectar as tabelas.
   */
  final String field;

  /**
   * Define a função representada por este {@link RFWField}.
   */
  final FieldFunction function;

  /**
   * Lista com os parâmetros da funcção definida, na ordem em que precisam ser passados.<br>
   * A lista aceita sempre um {@link RFWField} o que permite ter funções aninhadas e definição do nome da coluna ou constantes.<br>
   */
  final LinkedList<RFWField> functionParam;

  /**
   * Quando o tipo do {@link RFWField} for algum tipo de Constante, esse valor deve ser preenchido com o objeto de natureza equivalente à constante definida.
   */
  final Object constantValue;

  /**
   * Construtor privado pois o objeto deve sempre ser criado pelos métodos de criação.
   */
  private RFWField(FieldFunction function, String field, LinkedList<RFWField> params, Object constantValue) {
    this.function = function;
    this.field = field;
    this.functionParam = params;
    this.constantValue = constantValue;
  }

  /**
   * Cria um field de campo, sem qualquer função encapsulado diretamente.
   *
   * @param field Caminho da coluna a ser utilizada.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField field(String field) {
    return new RFWField(FieldFunction.FIELD, field, null, null);
  }

  /**
   * Cria um campo com a constante "NULL".
   *
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField constantNULL() {
    return new RFWField(FieldFunction.CONSTANT_NULL, null, null, null);
  }

  /**
   * Cria a função SUM para sumarizar valores de uma coluna numérica do banco de dados.
   *
   * @param field Caminho da coluna a ser sumarizada.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField sum(String field) {
    return new RFWField(FieldFunction.SUM, field, null, null);
  }

  /**
   * Cria a função SUM para sumarizar valores de uma coluna numérica do banco de dados.
   *
   * @param field Caminho da coluna a ser sumarizada.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField sum(RFWField field) {
    LinkedList<RFWField> list = new LinkedList<RFWField>();
    list.add(field);
    return new RFWField(FieldFunction.SUM, null, list, null);
  }

  /**
   * Cria a função Mínimo.
   *
   * @param field Caminho da coluna.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField minimum(String field) {
    return new RFWField(FieldFunction.MINIMUM, field, null, null);
  }

  /**
   * Cria a função Mínimo.
   *
   * @param field Caminho da coluna.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField minimum(RFWField field) {
    LinkedList<RFWField> list = new LinkedList<RFWField>();
    list.add(field);
    return new RFWField(FieldFunction.MINIMUM, null, list, null);
  }

  /**
   * Cria a função aritmética de Multiplicação.
   *
   * @param field1 Caminho da coluna1.
   * @param field2 Caminho da coluna2.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField multiply(RFWField field1, RFWField field2) {
    LinkedList<RFWField> list = new LinkedList<RFWField>();
    list.add(field1);
    list.add(field2);
    return new RFWField(FieldFunction.MULTIPLY, null, list, null);
  }

  /**
   * Cria a função aritmética de Divisão.
   *
   * @param field1 Caminho da coluna1.
   * @param field2 Caminho da coluna2.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField divide(RFWField field1, RFWField field2) {
    LinkedList<RFWField> list = new LinkedList<RFWField>();
    list.add(field1);
    list.add(field2);
    return new RFWField(FieldFunction.DIVIDE, null, list, null);
  }

  /**
   * Cria a função Máximo.
   *
   * @param field Caminho da coluna.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField maximum(String field) {
    return new RFWField(FieldFunction.MAXIMUM, field, null, null);
  }

  /**
   * Cria a função MONTH para obter o mês de uma coluna de data.
   *
   * @param field Caminho da coluna a ser sumarizada.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField month(String field) {
    return new RFWField(FieldFunction.MONTH, field, null, null);
  }

  /**
   * Cria a função YEAR para obter o mês de uma coluna de data.
   *
   * @param field Caminho da coluna a ser sumarizada.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField year(String field) {
    return new RFWField(FieldFunction.YEAR, field, null, null);
  }

  /**
   * Cria a função MONTH para obter o mês de uma coluna de data.
   *
   * @param field Caminho da coluna a ser sumarizada.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField month(RFWField field) {
    LinkedList<RFWField> list = new LinkedList<RFWField>();
    list.add(field);
    return new RFWField(FieldFunction.MONTH, null, list, null);
  }

  /**
   * Cria a função YEAR para obter o mês de uma coluna de data.
   *
   * @param field Caminho da coluna a ser sumarizada.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField year(RFWField field) {
    LinkedList<RFWField> list = new LinkedList<RFWField>();
    list.add(field);
    return new RFWField(FieldFunction.YEAR, null, list, null);
  }

  /**
   * Cria a função CONCAT para "juntar" os valores obeitdos.<Br>
   *
   * @param fields Caminho da(s) coluna(s) a ser utilizada(s).
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField concat(RFWField... fields) {
    LinkedList<RFWField> list = new LinkedList<RFWField>();
    for (RFWField f : fields) {
      list.add(f);
    }
    return new RFWField(FieldFunction.CONCAT, null, list, null);
  }

  /**
   * Cria a função COALESCE para obter os valores.<Br>
   * A função coalesce recupera o valor da primeira coluna passada, se este for nulo ele retornará o valor da seguinte. E assim por diante até encontrar um valor que não seja nulo. Caso todos sejam, o valor nulo é retornado igualmente.
   *
   * @param fields Caminho da(s) coluna(s) a ser utilizada(s).
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField coalesce(RFWField... fields) {
    LinkedList<RFWField> list = new LinkedList<RFWField>();
    for (RFWField f : fields) {
      list.add(f);
    }
    return new RFWField(FieldFunction.COALESCE, null, list, null);
  }

  /**
   * Cria a função COALESCE para obter os valores.<Br>
   * A função coalesce recupera o valor da primeira coluna passada, se este for nulo ele retornará o valor da seguinte. E assim por diante até encontrar um valor que não seja nulo. Caso todos sejam, o valor nulo é retornado igualmente.
   *
   * @param fields Caminho da(s) coluna(s) a ser utilizada(s).
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField coalesce(String... fields) {
    LinkedList<RFWField> list = new LinkedList<RFWField>();
    for (String f : fields) {
      list.add(new RFWField(FieldFunction.FIELD, f, null, null));
    }
    return new RFWField(FieldFunction.COALESCE, null, list, null);
  }

  /**
   * Cria uma constante do tipo texto para ser colocada no SQL.<Br>
   *
   * @param value Valor constante a ser utilizado.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField constantString(String value) {
    return new RFWField(FieldFunction.CONSTANTE_STRING, null, null, value);
  }

  /**
   * Cria a função COUNT para contar a quantidade de Linhas que foram retornadas.
   *
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField count() {
    return new RFWField(FieldFunction.COUNT, null, null, null);
  }

  /**
   * Obtem uma lista dos parâmetros que foram influídos nesta função.
   *
   * @return Lista clonada dos parâmetros.
   */
  @SuppressWarnings("unchecked")
  public LinkedList<RFWField> getFunctionParam() {
    return (LinkedList<RFWField>) this.functionParam.clone();
  }

  /**
   * Recupera o nome da coluna, quando a função foi criada diretamente com um nome de coluna (e não como uma função aninhada).<br>
   * Permite que o {@link RFWDAO} consiga saber que o valor é um caminho de coluna (utilizando a estrutura do Match Objetc) para saber como conectar as tabelas.
   *
   * @return the nome da coluna, quando a função foi criada diretamente com um nome de coluna (e não como uma função aninhada)
   */
  public String getField() {
    return field;
  }

  /**
   * Recupera a lista com todos os atributos usados no MO, incluindo dos SubMOs, caso existam.
   *
   * @return Lista com todos os atributos.
   */
  public List<String> getAttributes() {
    final LinkedList<String> list = new LinkedList<>();

    if (this.field != null) list.add(this.field);
    if (this.functionParam != null) {
      for (Object obj : this.functionParam) {
        if (obj instanceof RFWField) {
          list.addAll(((RFWField) obj).getAttributes());
        }
      }
    }
    return list;
  }

  public FieldFunction getFunction() {
    return function;
  }

  public Object getConstantValue() {
    return constantValue;
  }

  /**
   * Cria uma constante do tipo numérica para ser colocada no SQL.<Br>
   *
   * @param value Valor constante a ser utilizado.
   * @return Objeto montado indicando a função selecionada.
   */
  public static RFWField constantNumber(BigDecimal value) {
    return new RFWField(FieldFunction.CONSTANTE_NUMBER, null, null, value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(constantValue, field, function, functionParam);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    RFWField other = (RFWField) obj;
    return Objects.equals(constantValue, other.constantValue) && Objects.equals(field, other.field) && function == other.function && Objects.equals(functionParam, other.functionParam);
  }

}
