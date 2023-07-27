package br.eng.rodrigogml.rfw.orm.vo;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Objects;

/**
 * Description: Classe usada para definir os campos de ordenação de uma consulta com filtro.<br>
 *
 * @author Rodrigo Leitão
 * @since 3.2.0 (DEZ / 2009)
 */

public class RFWOrderBy implements Serializable {

  private static final long serialVersionUID = -125705213589524308L;

  public static class RFWOrderbyItem implements Serializable {

    private static final long serialVersionUID = 338553520436915356L;

    private RFWField field = null;
    private boolean asc = true;

    public RFWOrderbyItem(String column) {
      this(column, true);
    }

    public RFWOrderbyItem(String column, boolean asc) {
      this.field = RFWField.field(column);
      this.asc = asc;
    }

    public RFWOrderbyItem(RFWField field) {
      this(field, true);
    }

    public RFWOrderbyItem(RFWField field, boolean asc) {
      this.field = field;
      this.asc = asc;
    }

    public boolean isAsc() {
      return asc;
    }

    public void setAsc(boolean asc) {
      this.asc = asc;
    }

    public RFWField getField() {
      return field;
    }

    public void setField(RFWField field) {
      this.field = field;
    }

    @Override
    public int hashCode() {
      return Objects.hash(asc, field);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      RFWOrderbyItem other = (RFWOrderbyItem) obj;
      return asc == other.asc && Objects.equals(field, other.field);
    }
  }

  private final LinkedList<RFWOrderbyItem> orderByList = new LinkedList<>();

  private RFWOrderBy() {
  }

  private RFWOrderBy(String column) {
    addOrderbyItem(column);
  }

  public RFWOrderBy(String column, boolean asc) {
    addOrderbyItem(column, asc);
  }

  private RFWOrderBy(RFWField field) {
    addOrderbyItem(field);
  }

  public RFWOrderBy(RFWField field, boolean asc) {
    addOrderbyItem(field, asc);
  }

  public RFWOrderBy addOrderbyItem(RFWOrderbyItem item) {
    orderByList.add(item);
    return this;
  }

  public RFWOrderBy addOrderbyItem(String column) {
    orderByList.add(new RFWOrderbyItem(column));
    return this;
  }

  public RFWOrderBy addOrderbyItem(String column, boolean asc) {
    orderByList.add(new RFWOrderbyItem(column, asc));
    return this;
  }

  public RFWOrderBy addOrderbyItem(RFWField field) {
    orderByList.add(new RFWOrderbyItem(field));
    return this;
  }

  public RFWOrderBy addOrderbyItem(RFWField field, boolean asc) {
    orderByList.add(new RFWOrderbyItem(field, asc));
    return this;
  }

  /**
   * Cria um {@link RFWOrderBy} Vazio, que não cria nenhum orderby no fim das contas. Útil quando vamos adicionar colunas para ordenação dinamicamente, podendo utilizar um objeto "empty()" para começar a colucar (ou não) novas colunas de order by sem ter que testar se o objeto já foi instanciado ou não.
   */
  public static RFWOrderBy createEmpty() {
    return new RFWOrderBy();
  }

  public static RFWOrderBy createInstance(String column, boolean asc) {
    return new RFWOrderBy(column, asc);
  }

  public static RFWOrderBy createInstance(String column) {
    return new RFWOrderBy(column);
  }

  public static RFWOrderBy createInstance(RFWField field, boolean asc) {
    return new RFWOrderBy(field, asc);
  }

  public static RFWOrderBy createInstance(RFWField field) {
    return new RFWOrderBy(field);
  }

  /**
   * @return the orderbylist
   */
  public LinkedList<RFWOrderbyItem> getOrderbylist() {
    return orderByList;
  }

  /**
   * @param orderbylist the orderbylist to set
   */
  public void setOrderbylist(LinkedList<RFWOrderbyItem> orderbylist) {
    this.orderByList.clear();
    this.orderByList.addAll(orderbylist);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof RFWOrderBy)) {
      return false;
    }
    RFWOrderBy o2 = (RFWOrderBy) obj;
    if (o2.orderByList.size() != this.orderByList.size()) return false;

    for (int i = 0; i < this.orderByList.size(); i++) {
      RFWOrderbyItem i1 = this.orderByList.get(i);
      RFWOrderbyItem i2 = o2.orderByList.get(i);
      if (!i1.getField().equals(i2.getField()) || i1.isAsc() != i2.isAsc()) return false;
    }

    return true;
  }

  /**
   * Recupera todos os atributos utilizados no {@link RFWOrderBy}. Incluindo os atributos utilizados nas ordenações secundárias.
   */
  public LinkedList<String> getAttributes() {
    final LinkedList<String> atts = new LinkedList<>();
    for (RFWOrderbyItem orderbyItem : orderByList) {
      atts.addAll(orderbyItem.getField().getAttributes());
    }
    return atts;
  }
}
