package br.eng.rodrigogml.rfw.orm.dao;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import br.eng.rodrigogml.rfw.kernel.RFW;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWCriticalException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.logger.RFWLogger;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaCollectionField;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaEncrypt;
import br.eng.rodrigogml.rfw.kernel.utils.RUEncrypter;
import br.eng.rodrigogml.rfw.kernel.utils.RUReflex;
import br.eng.rodrigogml.rfw.kernel.vo.RFWField;
import br.eng.rodrigogml.rfw.kernel.vo.RFWMO;
import br.eng.rodrigogml.rfw.kernel.vo.RFWMO.RFWMOData;
import br.eng.rodrigogml.rfw.kernel.vo.RFWOrderBy;
import br.eng.rodrigogml.rfw.kernel.vo.RFWOrderBy.RFWOrderbyItem;
import br.eng.rodrigogml.rfw.kernel.vo.RFWVO;
import br.eng.rodrigogml.rfw.orm.dao.RFWDAO.SQLDialect;
import br.eng.rodrigogml.rfw.orm.dao.annotations.dao.RFWDAOConverter;
import br.eng.rodrigogml.rfw.orm.dao.interfaces.RFWDAOConverterInterface;

/**
 * Description: Classe que cria um mapeamento para execu��o de um comando SQL entre os objetos e o banco de dados.<br>
 *
 * @author Rodrigo Leit�o
 * @since 10.0.0 (13 de jul de 2018)
 */
class DAOMap {

  /**
   * Classe utilizada para registrar o mapeamento dos caminhos e as tabelas do sistemas.
   */
  static class DAOMapTable {

    /**
     * Classe do objeto cuja tabela sendo mapeada.
     */
    Class<? extends RFWVO> type = null;

    /**
     * Caminho do atributo, vazio quando o atributo est� no objeto raiz. Nulo quando � apenas uma tabela de joinAlias de N:N.
     */
    String path = null;

    /**
     * Schema do banco de dados em que esta tabela ser� encontrada.
     */
    String schema = null;

    /**
     * Tabela do banco de dados.
     */
    String table = null;

    /**
     * Alias da tabela no SQL. Segue a sequ�ncia t0, t1, t2, ..., tN
     */
    String alias = null;

    /**
     * Nulo na tabela t0, que representa o come�o do SQL e a tabela do objeto raiz. Para as demais tabelas anexadas ao SQL deve conter o Alias da tabela � qual esta est� anexada.
     */
    String joinAlias = null;

    /**
     * Nulo na tabela t0, que representa o come�o do SQL e a tabela do objeto raiz. Para as demais tabelas anexadas, contem a coluna desta tabela que ser� usada para fazer o joinAlias com a tabela descrita em {@link #joinAlias}.
     */
    String column = null;

    /**
     * Nulo na tabela t0, que representa o come�o do SQL e a tabela do objeto raiz. Para as demais tabelas anexadas, contem a coluna da tabela descrita em {@link #joinAlias} que ser� usada para fazer o joinAlias com esta tabela.
     */
    String joinColumn = null;

    /**
     * Lista das tabelas que fazem join nesta tabela.<Br>
     * De outro ponto de vista, todos os {@link DAOMapTable} cujo atributo {@link DAOMapTable#joinAlias} referencia o {@link DAOMapTable#alias} deste objeto.
     */
    final List<DAOMapTable> joinedTables = new LinkedList<DAOMapTable>();

    /**
     * Lista dos {@link DAOMapField} que foram mapeados desta tabela.<bR>
     * Ou seja, todos os {@link DAOMapField} cujo atributo {@link DAOMapField#table} aponta para este objeto.
     */
    final List<DAOMapField> mappedFields = new LinkedList<DAOMapField>();
  }

  /**
   * Classe utilizada para registrar o mapeamento dos caminhos e as colunas das tabelas do sistema.
   */
  static class DAOMapField {

    /**
     * Caminho do atributo a partir do objeto raiz. Vazio para os campos do objeto raiz. N�o inclu� o nome do atributo do objeto, este deve ser declarado em {@link #field}.
     */
    String path = null;

    /**
     * Nome do atributo do objeto sendo mapeado
     */
    String field = null;

    /**
     * nome da coluna no banco de dados.
     */
    String column = null;

    /**
     * Tabela � qual este campo est� associado
     */
    DAOMapTable table = null;
  }

  /**
   * Mant�m refer�ncia para a tabela "raiz", a primeira tabela do SELECT, com alias t0.
   */
  private DAOMapTable rootTable = null;

  /**
   * Hash com os mapas de tabela indexados pelo caminho.<BR>
   * <b>ATEN��O: os mapeamentos de tabelas de N:N que n�o tem "path" s�o colocados na hash com uma chave gerada com o seguinte formato: { '.' + <alias> + '.' + <joinAlias> } para garantir a unicidade da chave.</b>
   */
  private final LinkedHashMap<String, DAOMapTable> mapTableByPath = new LinkedHashMap<>();

  /**
   * Hash com os mapas de tabela indexados pelo alias.<BR>
   */
  private final LinkedHashMap<String, DAOMapTable> mapTableByAlias = new LinkedHashMap<>();

  /**
   * Hash com os mapas dos campos indexados pelo caminho. <br>
   */
  private final LinkedHashMap<String, DAOMapField> mapFieldByPath = new LinkedHashMap<>();

  DAOMap() {
  }

  /**
   * Cria um novo mapeamento entre VO e Tabela do Banco de dados.
   *
   * @param type Classe do objeto / entidade do sistema.
   * @param path Caminho at� esta entidade a partir do objeto raiz. Usar "" para o objeto raiz. Para tabelas de N:N que n�o tem objeto associado, utilizar o caminho do objeto N:N precedido de um '.', para que n�o se confunda com o path da tabela do pr�prio objeto.
   * @param schema Schema onde a tabela se encontra.
   * @param table Tabela do Banco de Dados que armazena os dados dessa entidade.
   * @param column Coluna na tabela desta entidade que � utilizada para realizar o Join.
   * @param joinAlias Alias da tabela na qual esta far� o joinAlias. Null para a tabela raiz.
   * @param joinColumn Coluna na outra tabela (informada em 'joinAlias') utilizada para fazer o joinAlias.
   * @return Objeto de mapeamento entre Banco de Dados e Objeto do sistema.
   * @throws RFWException
   */
  public DAOMapTable createMapTable(Class<? extends RFWVO> type, String path, String schema, String table, String column, String joinAlias, String joinColumn) throws RFWException {
    return createMapTable(type, path, schema, table, column, joinAlias, joinColumn, "t" + this.mapTableByPath.size());
  }

  /**
   * Cria um novo mapeamento entre VO e Tabela do Banco de dados.
   *
   * @param type Classe do objeto / entidade do sistema.
   * @param path Caminho at� esta entidade a partir do objeto raiz. Usar "" para o objeto raiz. Para tabelas de N:N que n�o tem objeto associado, utilizar o caminho do objeto N:N precedido de um '.', para que n�o se confunda com o path da tabela do pr�prio objeto.
   * @param schema Schema onde a tabela se encontra.
   * @param table Tabela do Banco de Dados que armazena os dados dessa entidade.
   * @param column Coluna na tabela desta entidade que � utilizada para realizar o Join.
   * @param joinAlias Alias da tabela na qual esta far� o joinAlias. Null para a tabela raiz.
   * @param joinColumn Coluna na outra tabela (informada em 'joinAlias') utilizada para fazer o joinAlias.
   * @param alias For�a a defini��o de um Alias personalizado. Utilizado somente para casos de clone.
   * @return Objeto de mapeamento entre Banco de Dados e Objeto do sistema.
   * @throws RFWException
   */
  private DAOMapTable createMapTable(Class<? extends RFWVO> type, String path, String schema, String table, String column, String joinAlias, String joinColumn, String alias) throws RFWException {
    if (this.mapTableByAlias.containsKey(alias) || this.mapTableByPath.containsKey(path)) {
      throw new RFWCriticalException("Alias j� existente neste DAOMap!");
    }

    DAOMapTable t = new DAOMapTable();
    t.type = type;
    t.alias = alias;
    t.schema = schema;
    t.table = table;
    t.joinAlias = joinAlias;
    t.column = column;
    t.joinColumn = joinColumn;
    t.path = path;

    if (this.mapTableByAlias.size() == 0) this.rootTable = t;
    this.mapTableByPath.put(path, t);
    this.mapTableByAlias.put(t.alias, t);

    if (t.joinAlias != null) this.mapTableByAlias.get(t.joinAlias).joinedTables.add(t);

    return t;
  }

  /**
   * Acrescenta o mapeamento de mais uma tabela Replica o mapeamento de uma tabela existente para um caminho (entidade) para outro caminho (outra entidade similar).<br>
   * Essa replica��o � �til quando precisamos modificar dinamicamente o DAOMap para iteragir com objetos hierarquicos.
   *
   * @param originalPath Caminho original (j� existente) para ser replicado. Normalmente o caminho do primeiro objeto da estrutura hierarquica.
   * @param destinationPath Caminho final (a ser duplicado) para o mesmo objeto e que utilizar� as defini��es do objeto original, para que o caminho seja replicado.
   * @param column Coluna na tabela desta entidade que � utilizada para realizar o Join.
   * @param joinColumn Coluna na outra tabela (informada em 'joinAlias') utilizada para fazer o joinAlias.
   * @return Objeto de mapeamento duplicado.
   * @throws RFWException
   */
  public DAOMapTable createMapTableForCompositionTree(String originalPath, String destinationPath, String column, String joinColumn) throws RFWException {
    return createMapTableForCompositionTree(originalPath, destinationPath, column, joinColumn, null, null);
  }

  /**
   * @param Define o alias base, da primeira tabela sendo replicada, para evitar de entrar em loop eterno replicando as associa��es rec�m criadas
   */
  private DAOMapTable createMapTableForCompositionTree(String originalPath, String destinationPath, String column, String joinColumn, DAOMapTable parentTable, String baseAlias) throws RFWException {
    DAOMapTable origTable = this.mapTableByPath.get(originalPath);
    if (origTable != null) {
      DAOMapTable newTable = this.mapTableByPath.get(destinationPath);
      if (newTable == null) {
        // Se ainda n�o existir a tabela criamos. Se existir utilizamos a existente com seu alias, mas vamos completar. Isso pq o primeiro n�vel de objetos hierarquivos j� � mapeado, s� seus subobjetos que n�o.
        String joinAlias = origTable.alias;
        if (parentTable != null) {
          joinAlias = parentTable.alias;
        }
        newTable = createMapTable(origTable.type, destinationPath, origTable.schema, origTable.table, origTable.column, joinAlias, joinColumn, "tree" + this.mapTableByPath.size());
      }

      // Buscamos todos os atributos associadoa a tabela base, para replicar para este novo mapeamento
      for (DAOMapField mapField : new ArrayList<>(getMapField())) {
        if (mapField.table == origTable) {
          // Verifica se o campo ainda n�o existe.
          if (this.getMapFieldByPath(destinationPath, mapField.field) == null) {
            createMapField(destinationPath, mapField.field, newTable, mapField.column);
          }
        }
      }

      // Busca todas as tabelas que fazem JOIN com a tabela recebida (que s�o os subobjetos da �rvore) e replicamos
      for (DAOMapTable mTable : new ArrayList<>(getMapTable())) {
        if (origTable.alias.equals(mTable.joinAlias)) { // S� duplica as tabelas com Join direto
          if (newTable != mTable) { // N�o deixa entrar em Loop replicando a pr�pria tabela
            if (!mTable.alias.equals(baseAlias)) {
              if (baseAlias == null) baseAlias = newTable.alias;
              if (!this.mapTableByPath.containsKey(destinationPath + mTable.path.substring(originalPath.length()))) {
                createMapTableForCompositionTree(mTable.path, destinationPath + mTable.path.substring(originalPath.length()), mTable.column, mTable.joinColumn, newTable, baseAlias);
              }
            }
          }
        }
      }
      return newTable;
    }
    return null;
  }

  /**
   * Cria um mapeamnto entre um 'field' de um objeto do sistema e uma coluna da tabela do banco de dados.
   *
   * @param path Caminho no objeto desde o objeto raiz at� o objeto deste atributo. N�o inclui o atributo deste objeto. Utilizar "" para o objeto raiz.
   * @param field Nome do field no objeto sendo mapeado.
   * @param table Objeto mapeamento da entidade na tabela do sistema.
   * @param column Coluna do banco de dados onde o campo ser� associado.
   * @return Objeto criado de mapeamento entre o field e a coluna da tabela.
   */
  public DAOMapField createMapField(String path, String field, DAOMapTable table, String column) {
    DAOMapField t = new DAOMapField();
    t.path = path;
    t.field = field;
    t.table = table;
    t.column = column;

    if (path.length() > 0 && !"@".equals(path)) path += ".";
    path += field;

    table.mappedFields.add(t);

    this.mapFieldByPath.put(path, t);
    return t;
  }

  /**
   * Retorna um mapeamento de tabela a partir do Path definido.
   *
   * @param path Caminho desde o objeto raiz at� este objeto.
   * @return Mapeamento da Tabela
   */
  public DAOMapTable getMapTableByPath(String path) {
    return this.mapTableByPath.get(path);
  }

  /**
   * Retorna um mapeamento de tabela a partir do Alias utilizado na tabela.
   *
   * @param alias Apelido da tabela no SQL. Criado automaticamente no m�todo {@link #createMapTable(Class, String, String, String, String, String, String)}.
   * @return Mapeamento da Tabela
   */
  public DAOMapTable getMapTableByAlias(String alias) {
    return this.mapTableByAlias.get(alias);
  }

  /**
   * Retorna Cole��o com todos os Mapeamentos de Tabelas do DAOMap
   */
  public Collection<DAOMapTable> getMapTable() {
    return this.mapTableByPath.values();
  }

  /**
   * Recupera um mapeamento entre field e coluna do banco de dados.
   *
   * @param path Caminho desde o objeto raiz at� o este objeto. N�o inclui o nome do atributo.
   * @param fieldName field/atributo do objeto.
   * @return Objeto de Mapeamento entre field e coluna do banco de dados
   */
  public DAOMapField getMapFieldByPath(String path, String fieldName) {
    if (path.length() > 0) path += ".";
    path += fieldName;
    return this.mapFieldByPath.get(path);
  }

  /**
   * Recupera um mapeamento entre field e coluna do banco de dados pelo caminho/nome completo.
   *
   * @param fullPath Caminho desde o objeto raiz at� o este objeto. INCLUI o nome do atributo.
   * @return Objeto de Mapeamento entre field e coluna do banco de dados
   */
  public DAOMapField getMapFieldByPath(String fullPath) {
    return this.mapFieldByPath.get(fullPath);
  }

  /**
   * Cole��o de objetos de mapeamento entre field dos VOs e colunas das tabelas.
   */
  public Collection<DAOMapField> getMapField() {
    return this.mapFieldByPath.values();
  }

  /**
   * Cria o Statement SQL para consulta baseado em no Map recebido.<br>
   * Mesmo que o m�todo {@link #createMapTable(Class, String, String, String, String, String, String)} com o attributo selectFields nulo.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param mo Objeto com as confi��es da Clausula WHERE a ser utilizada.
   * @param orderBy Objeto com as informa��es de ordena��o. Passar Null para n�o defir nenhum OrderBy.
   * @param useFullJoin Caso TRUE, o SELECTED ser� montado com FULL JOIN ao inv�s do LEFT JOIN
   * @param dialect
   * @return PreparedStatemetn pronto para realizar a consulta e obter o ResultSet.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static PreparedStatement createSelectStatement(Connection conn, DAOMap map, RFWMO mo, RFWOrderBy orderBy, Boolean useFullJoin, SQLDialect dialect) throws RFWException {
    return createSelectStatement(conn, map, null, null, false, mo, orderBy, null, null, null, useFullJoin, dialect);
  }

  /**
   * Cria o Statement SQL para consulta baseado em no Map recebido.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param selectFields Campos que devem ser obtidos das tabelas. Se passo nulo, ser� obtido todos os campos que est�o no objeto de mapeamento.
   * @param expandTable Este atributo s� � utilizando quando o selectFields n�o � nulo. Caso TRUE obtem todos os campos de uma tabela mesmo que s� uma coluna esteja definida no selectFields. Caso FALSE recupera estritamente os campos passados em selectFields.
   * @param mo Objeto com as confi��es da Clausula WHERE a ser utilizada.
   * @param orderBy Objeto que define a ordem da lista.
   * @param offSet Define quantos registros a partir do come�o devemos pular (n�o retornar), por exemplo, um offSet de 0, retorna desde o primeiro (index 0).
   * @param limit Define quantos registros devemos retornar da lista. Define a quantidade e n�o o index como o "offSet". Se ambos forem combinados, ex: offset = 5 e limit = 10, retorna os registros desde o index 5 at� o idnex 15, ou seja da 6� linha at� a 15�.
   * @param useFullJoin Caso TRUE, o SELECTED ser� montado com FULL JOIN ao inv�s do LEFT JOIN
   * @param dialect
   * @return PreparedStatemetn pronto para realizar a consulta e obter o ResultSet.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static PreparedStatement createSelectStatement(Connection conn, DAOMap map, String[] selectFields, boolean expandTable, RFWMO mo, RFWOrderBy orderBy, Integer offSet, Integer limit, Boolean useFullJoin, SQLDialect dialect) throws RFWException {
    return createSelectStatement(conn, map, null, selectFields, expandTable, mo, orderBy, null, offSet, limit, useFullJoin, dialect);
  }

  /**
   * Cria o Statement SQL para consulta baseado em no Map recebido.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param fields Campos com fun��es para serem recuperados.
   * @param mo Objeto com as confi��es da Clausula WHERE a ser utilizada.
   * @param orderBy Objeto que define a ordem da lista.
   * @param offSet Define quantos registros a partir do come�o devemos pular (n�o retornar), por exemplo, um offSet de 0, retorna desde o primeiro (index 0).
   * @param limit Define quantos registros devemos retornar da lista. Define a quantidade e n�o o index como o "offSet". Se ambos forem combinados, ex: offset = 5 e limit = 10, retorna os registros desde o index 5 at� o idnex 15, ou seja da 6� linha at� a 15�.
   * @param useFullJoin Caso TRUE, o SELECTED ser� montado com FULL JOIN ao inv�s do LEFT JOIN
   * @param dialect
   * @return PreparedStatemetn pronto para realizar a consulta e obter o ResultSet.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static PreparedStatement createSelectStatement(Connection conn, DAOMap map, RFWField[] fields, RFWMO mo, RFWOrderBy orderBy, RFWField[] groupBy, Integer offSet, Integer limit, Boolean useFullJoin, SQLDialect dialect) throws RFWException {
    return createSelectStatement(conn, map, fields, null, false, mo, orderBy, groupBy, offSet, limit, useFullJoin, dialect);
  }

  /**
   * Cria o Statement SQL para consulta baseado em no Map recebido.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param fields Campos com fun��es para serem recuperados. Note que ao informar esse par�metro ser�o ignorados os par�metros: selectFields e expandTable
   * @param selectFields Campos que devem ser obtidos das tabelas. Se passo nulo, ser� obtido todos os campos que est�o no objeto de mapeamento.
   * @param expandTable Este atributo s� � utilizando quando o selectFields n�o � nulo. Caso TRUE obtem todos os campos de uma tabela mesmo que s� uma coluna esteja definida no selectFields. Caso FALSE recupera estritamente os campos passados em selectFields.
   * @param mo Objeto com as confi��es da Clausula WHERE a ser utilizada.
   * @param orderBy Objeto que define a ordem da lista.
   * @param groupBy Campos com fun��es ou fields/colunas a serem utilizadas no groupBy da consulta.
   * @param offSet Define quantos registros a partir do come�o devemos pular (n�o retornar), por exemplo, um offSet de 0, retorna desde o primeiro (index 0).
   * @param limit Define quantos registros devemos retornar da lista. Define a quantidade e n�o o index como o "offSet". Se ambos forem combinados, ex: offset = 5 e limit = 10, retorna os registros desde o index 5 at� o idnex 15, ou seja da 6� linha at� a 15�.
   * @param useFullJoin Caso TRUE, o SELECTED ser� montado com FULL JOIN ao inv�s do LEFT JOIN
   * @return PreparedStatemetn pronto para realizar a consulta e obter o ResultSet.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  private static PreparedStatement createSelectStatement(Connection conn, DAOMap map, RFWField[] fields, String[] selectFields, boolean expandTable, RFWMO mo, RFWOrderBy orderBy, RFWField[] groupBy, Integer offSet, Integer limit, Boolean useFullJoin, SQLDialect dialect) throws RFWException {
    final StringBuffer sql = new StringBuffer();

    try {
      // Se tiver a defini��o dos RFWFields vamos utilizar ela para realizar a montagem do SELECT, e o selectFields � ignorado. Caso contr�rio a consulta � feita em cima do selectFields
      if (fields != null) {
        for (RFWField field : fields) {
          if (sql.length() > 0) {
            sql.append(",");
          } else {
            sql.append("SELECT ");
          }
          sql.append(evalRFWField(map, field, dialect));
        }
      } else {
        // ==> SELECT
        {
          // Se recebemos os campos espec�ficos para buscar, recuperamos apenas eles, caso contr�rio vamos recuperar todas as tabelas mapeadas.
          if (selectFields == null) {
            // Se n�o temos nenhum field, n�o adicionamos nada al�m da tabela raiz completa
            sql.append("SELECT ").append(dialect.getQM()).append("t0").append(dialect.getQM()).append(".*");
          } else {
            // CACHE para armazena as tabelas que j� adicionamos para evitar de repeti-las no select. Chave � o Alias da tabela.
            // ATEN��O: Quando o expandTable � false, no cache � colocado o fieldPath Completo da tabela, evitando assim que as colunas sejam solicitadas repetidas
            HashMap<String, DAOMapTable> cache = new HashMap<>();

            // A tabela raiz sempre � utilizada, seja para pegar o ID do objeto, seja para pegar a tabela toda no caso de expandTable = true
            if (expandTable) {
              // Se vamos expandir a tabela, j� colocamos ela no select e adicionamos no cache para que n�o se repita
              sql.append("SELECT ").append(dialect.getQM()).append("t0").append(dialect.getQM()).append(".*");
              cache.put("t0", map.getMapTableByPath(""));
            } else {
              // Se N�O vamos expandir a tabela, s� colocamos o campo id inicialmente
              sql.append("SELECT ").append(dialect.getQM()).append("t0").append(dialect.getQM()).append(".").append(dialect.getQM()).append("id").append(dialect.getQM());
              cache.put("t0.id", map.getMapTableByPath(""));
            }

            for (String field : selectFields) {
              // Se o field solicitado for "id", ignoramos pois j� incluimos ele seja com expandTable = true ou false.
              if ("id".equals(field)) continue;

              // Se o atributo sendo requisitado come�a com "@" � um atributo anotado com RFWMetaCollection, precisa ser tratado diferente
              boolean collection = false;
              if (field.endsWith("@")) {
                // Se for uma RFWMetaCollection, deixamos a flag para indicar
                collection = true;
              }

              // Como temos de pegar todos os objetos entre o objeto raiz e os campos selecionados, vamos quebrar cada campo solicitado para garantir que vamos pedir no SELECT os campos intermedi�rios
              String[] parts = field.split("\\.");
              StringBuilder pathBuilder = new StringBuilder();
              for (int i = 0; i < parts.length; i++) {
                if (i < parts.length - 1) {
                  if (pathBuilder.length() > 0) pathBuilder.append(".");
                  pathBuilder.append(parts[i]);
                }

                // Cria o nome completo do atributo. Mas se ainda n�o estiver no �ltimo parametro (�ltimo parts) colocamos o final .id ou n�o a tabela n�o ser� encontrada
                String fieldName = pathBuilder.toString();
                if (fieldName.length() > 0) fieldName += ".";
                if (collection || i >= parts.length - 1) {
                  fieldName += parts[parts.length - 1];
                } else {
                  fieldName += "id";
                }

                // Se for collection, a partir do caminho
                DAOMapField mField = map.mapFieldByPath.get(fieldName);
                if (mField == null) {
                  // Se n�o encontramos procuramos se o atributo n�o � um collection, mas solicitado pelo usu�rio (Path criado pelo MetaVO_)
                  mField = map.mapFieldByPath.get(fieldName + "@");
                  if (mField == null) {
                    // Um dos casos do mField ser nulo � pq o atributo solicitado no find n�o � um atributo "final" de um objeto, mas sim um atributo que aponta para outro objeto de relacionamento.
                    // No Find devemos sempre buscar os atributos diretos dos objetos, caso contr�rio eles n�o ser�o mapeados.
                    throw new RFWCriticalException("O atributo '${0}' n�o foi maepado no DAO! Ao realizar consultas, sempre utilize o caminho desejado at� um atributo do objeto e n�o somente para um atributo que aponte o relacionamento (VO). Em outras palavras, n�o termine o caminho desejado do MO com \".path()\", solicite um atributo do objeto como \".id()\".", new String[] { fieldName });
                  }
                }
                final DAOMapTable mTable = mField.table;
                if (mTable.path != null) {
                  if (expandTable) {
                    // Se vams expandir a tabela, adicionamos a tabela toda e colocamos o Alias no cache para que n�o se repita para outros campos
                    if (!cache.containsKey(mTable.alias)) {
                      sql.append(",").append(dialect.getQM()).append(mTable.alias).append(dialect.getQM()).append(".*");
                      cache.put(mTable.alias, mTable); // Coloca na cache para n�o repetir
                    }
                  } else {
                    // Se n�o expande toda a tabela, vamos adicionar apenas o field, e n�o colocamos no cache, j� que a mesma tabela pode se repetir para outro campo.
                    if (!cache.containsKey(mTable.alias)) {
                      sql.append(",").append(dialect.getQM()).append(mTable.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mField.column).append(dialect.getQM());
                      cache.put(mTable.alias + '.' + mField.column, mTable); // Coloca na cache para n�o repetir
                    }
                  }
                }
              }
            }
          }
        }
      }

      // ==> FROM
      {
        // Come�a incluindo o mapeamento raiz
        final DAOMapTable mTableRoot = map.mapTableByPath.get("");
        sql.append(" FROM ").append(dialect.getQM());
        if (mTableRoot.schema != null) {
          sql.append(mTableRoot.schema).append(dialect.getQM()).append(".").append(dialect.getQM());
        }
        sql.append(mTableRoot.table).append(dialect.getQM()).append(" AS ").append(dialect.getQM()).append(mTableRoot.alias).append(dialect.getQM());
        // Itera as demais tabelas para o Join
        for (DAOMapTable mTable : map.mapTableByPath.values()) {
          // Evita a tabela raiz, j� que ela j� foi adicionada
          if (!"".equals(mTable.path)) {
            if (useFullJoin == null || !useFullJoin) {
              sql.append(" LEFT JOIN ").append(dialect.getQM());
            } else {
              sql.append(" FULL JOIN ").append(dialect.getQM());
            }
            sql.append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" AS ").append(dialect.getQM()).append(mTable.alias).append(dialect.getQM()).append(" ON ").append(dialect.getQM()).append(mTable.joinAlias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.joinColumn).append(dialect.getQM()).append(" = ").append(dialect.getQM()).append(mTable.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.column).append(dialect.getQM());
          }
        }
      }

      // ==> WHERE
      final LinkedList<Object> statementParameters = new LinkedList<>();
      if (mo != null) {
        final StringBuilder where = writeWhere(map, mo, statementParameters, dialect);
        // As vezes temos MO, mas ele n�o gera nenhuma consulta (um MO em branco), neste caso n�o escrevemos o "WHERE" ou teremos um SQL inv�lido
        if (where.length() > 0) sql.append(" WHERE").append(where);
      }

      // ==> GroupBy
      if (fields != null && groupBy != null) { // S� � utilizado no caso de consulta especial com fields preparados
        final StringBuilder gbBuff = new StringBuilder();
        for (RFWField field : groupBy) {
          if (gbBuff.length() > 0) {
            gbBuff.append(",");
          } else {
            gbBuff.append(" GROUP BY ");
          }
          gbBuff.append(evalRFWField(map, field, dialect));
        }
        sql.append(gbBuff);
      }

      // ==> ORDERBY
      boolean first = true;
      if (orderBy != null) {
        for (RFWOrderbyItem orderItem : orderBy.getOrderbylist()) {
          if (first) {
            sql.append(" ORDER BY ");
            first = false;
          } else {
            sql.append(", ");
          }
          sql.append(evalRFWField(map, orderItem.getField(), dialect));
          // sql.append(dialect.getQM()).append(mField.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mField.column).append(dialect.getQM());
          if (!orderItem.isAsc()) sql.append(" DESC");
        }
        // Adicionamos no fim, como crit�rio final de desempate a organiza��o pelo ID da tabela principal. Esse atributo garante que chamadas diferentes no m�todo (com MOs diferentes) retornem sempre a mesma ordem mesmo que com objetos ocultos. Sem essa organiza��o, requisi��es por "chunks of data" retornam cada hora uma ordem dependendo do MO, repetindo dados e errando a distribui��o.
        // Note que essa solu��o s� � colocada em caso de utiliza��o do OrderBy, se n�o ouver Order By, n�o precisamos do desempate j� que a ordem n�o � importante.
        sql.append(", ").append(dialect.getQM()).append("t0").append(dialect.getQM()).append(".").append(dialect.getQM()).append("id").append(dialect.getQM());
      }

      // ==> LIMIT
      if (offSet != null || limit != null) {
        sql.append(" LIMIT ");
        if (offSet != null) sql.append(offSet).append(",");
        if (limit != null) {
          sql.append(limit);
        } else {
          // Se entrou nesse IF � pq temos um offSet, mas para usar o offset o MySQL exige o limit tamb�m. Por isso utilizmaos um numero gigantesco para garantir que tudo seja retornado. https://dev.mysql.com/doc/refman/8.0/en/select.html
          sql.append("18446744073709551615");
        }
      }

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      if (map.getMapTable().size() > 15) {
        // Se tivermos mais de 15 tabelas conectadas, ativamos o fetch de linha a linha para n�o termos problema de mem�ria. N�o deixamos direto pq o linha a linha � pior em quest�es de performance.
        RFW.pDev("Limite de Fetch do MySQL (Linha � Linha) habilitado!");
        stmt.setFetchSize(Integer.MIN_VALUE);
      }
      writeStatementParameters(stmt, statementParameters);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Cria o Statement SQL para completar objetos de CompositionTree.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param startTable tabela de in�cio de busca dos objetos
   * @param id ID do objeto a ser procurado na tabela
   * @param orderBy Objeto que define a ordem da lista.
   * @param groupBy Campos com fun��es ou fields/colunas a serem utilizadas no groupBy da consulta.
   * @param offSet Define quantos registros a partir do come�o devemos pular (n�o retornar), por exemplo, um offSet de 0, retorna desde o primeiro (index 0).
   * @param limit Define quantos registros devemos retornar da lista. Define a quantidade e n�o o index como o "offSet". Se ambos forem combinados, ex: offset = 5 e limit = 10, retorna os registros desde o index 5 at� o idnex 15, ou seja da 6� linha at� a 15�.
   * @param useFullJoin Caso TRUE, o SELECTED ser� montado com FULL JOIN ao inv�s do LEFT JOIN
   * @param dialect
   * @return PreparedStatemetn pronto para realizar a consulta e obter o ResultSet.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static PreparedStatement createSelectCompositionTreeStatement(Connection conn, DAOMap map, DAOMapTable startTable, Long id, RFWOrderBy orderBy, RFWField[] groupBy, Integer offSet, Integer limit, Boolean useFullJoin, SQLDialect dialect) throws RFWException {
    final StringBuffer sql = new StringBuffer();
    final StringBuffer sqlFrom = new StringBuffer();
    try {
      // ==> SELECT
      sql.append("SELECT ").append(dialect.getQM()).append(startTable.alias).append(dialect.getQM()).append(".*");

      // ==> FROM
      {
        // Come�a incluindo o mapeamento raiz
        sqlFrom.append(" FROM ").append(dialect.getQM()).append(startTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(startTable.table).append(dialect.getQM()).append(" AS ").append(dialect.getQM()).append(startTable.alias).append(dialect.getQM());
        // Adiciona todas as tabelas que fazem join com a nossa tabela recursivamente. (para evitar um m�todo recusivo fiz esse loop que fica reiterando at� que as altera��es parem
        HashSet<String> cacheAlias = new HashSet<String>();
        cacheAlias.add(startTable.alias);
        int size = -1;
        while (size != cacheAlias.size()) {
          size = cacheAlias.size();

          // Itera as demais tabelas para o Join
          for (DAOMapTable mTable : map.mapTableByPath.values()) {
            // Evita a tabela raiz, j� que ela j� foi adicionada, ou qualquer tabela que j� esteja no cache
            if (!cacheAlias.contains(mTable.alias) && cacheAlias.contains(mTable.joinAlias)) {
              sql.append(",").append(dialect.getQM()).append(mTable.alias).append(dialect.getQM()).append(".*");

              if (useFullJoin == null || !useFullJoin) {
                sqlFrom.append(" LEFT JOIN ").append(dialect.getQM());
              } else {
                sqlFrom.append(" FULL JOIN ").append(dialect.getQM());
              }
              sqlFrom.append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" AS ").append(dialect.getQM()).append(mTable.alias).append(dialect.getQM()).append(" ON ").append(dialect.getQM()).append(mTable.joinAlias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.joinColumn).append(dialect.getQM()).append(" = ").append(dialect.getQM()).append(mTable.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.column).append(dialect.getQM());
              cacheAlias.add(mTable.alias);
            }
          }
        }
      }

      sql.append(sqlFrom);

      // ==> WHERE
      final LinkedList<Object> statementParameters = new LinkedList<>();
      sql.append(" WHERE ").append(dialect.getQM()).append(startTable.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append("id").append(dialect.getQM()).append("=?");
      statementParameters.add(id);

      // // ==> GroupBy
      // if (fields != null && groupBy != null) { // S� � utilizado no caso de consulta especial com fields preparados
      // final StringBuilder gbBuff = new StringBuilder();
      // for (RFWField field : groupBy) {
      // if (gbBuff.length() > 0) {
      // gbBuff.append(",");
      // } else {
      // gbBuff.append(" GROUP BY ");
      // }
      // gbBuff.append(evalRFWField(map, field));
      // }
      // sql.append(gbBuff);
      // }
      //
      // // ==> ORDERBY
      // boolean first = true;
      // if (orderBy != null) {
      // for (RFWOrderbyItem orderItem : orderBy.getOrderbylist()) {
      // if (first) {
      // sql.append(" ORDER BY ");
      // first = false;
      // } else {
      // sql.append(", ");
      // }
      // sql.append(evalRFWField(map, orderItem.getField()));
      // // sql.append(dialect.getQM()).append(mField.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mField.column).append(dialect.getQM());
      // if (!orderItem.isAsc()) sql.append(" DESC");
      // }
      // // Adicionamos no fim, como crit�rio final de desempate a organiza��o pelo ID da tabela principal. Esse atributo garante que chamadas diferentes no m�todo (com MOs diferentes) retornem sempre a mesma ordem mesmo que com objetos ocultos. Sem essa organiza��o, requisi��es por "chunks of data" retornam cada hora uma ordem dependendo do MO, repetindo dados e errando a distribui��o.
      // // Note que essa solu��o s� � colocada em caso de utiliza��o do OrderBy, se n�o ouver Order By, n�o precisamos do desempate j� que a ordem n�o � importante.
      // sql.append(", ").append(dialect.getQM()).append("t0").append(dialect.getQM()).append(".").append(dialect.getQM()).append("id").append(dialect.getQM());
      // }
      //
      // // ==> LIMIT
      // if (offSet != null || limit != null) {
      // sql.append(" LIMIT ");
      // if (offSet != null) sql.append(offSet).append(",");
      // if (limit != null) {
      // sql.append(limit);
      // } else {
      // // Se entrou nesse IF � pq temos um offSet, mas para usar o offset o MySQL exige o limit tamb�m. POr isso utilizmaos um numero gigantesco para garantir que tudo seja retornado. https://dev.mysql.com/doc/refman/8.0/en/select.html
      // sql.append("18446744073709551615");
      // }
      // }

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      if (map.getMapTable().size() > 15) {
        // Se tivermos mais de 15 tabelas conectadas, ativamos o fetch de linha a linha para n�o termos problema de mem�ria. N�o deixamos direto pq o linha a linha � pior em quest�es de performance.
        RFWLogger.logDebug("Limite de Fetch do MySQL (Linha � Linha) habilitado!");
        stmt.setFetchSize(Integer.MIN_VALUE);
      }
      writeStatementParameters(stmt, statementParameters);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * M�todo utilizado para trocar a defini��o do {@link RFWField} na string necess�ria para anexar no SQL de consulta.
   *
   * @param map {@link DAOMap} com as informa��es de mapeamento
   * @param field {@link RFWField} para evaluation.
   * @param dialect
   * @return String pronta para ser concatenada na string SQL.
   */
  private static String evalRFWField(DAOMap map, RFWField field, SQLDialect dialect) {
    StringBuilder buff = new StringBuilder();

    switch (field.getFunction()) {
      case FIELD:
        buff.append(evalFieldToColumn(map, field.getField(), dialect));
        break;
      case SUM:
        if (field.getField() != null) {
          buff.append("sum(").append(evalFieldToColumn(map, field.getField(), dialect)).append(")");
        } else {
          buff.append("sum(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(")");
        }
        break;
      case SUBTRACT:
        buff.append("(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(" - ").append(evalRFWField(map, field.getFunctionParam().get(1), dialect)).append(")");
        break;
      case COUNT:
        buff.append("count(*)");
        break;
      case DISTINCT:
        if (field.getField() != null) {
          buff.append("distinct(").append(evalFieldToColumn(map, field.getField(), dialect)).append(")");
        } else {
          buff.append("distinct(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(")");
        }
        break;
      case COALESCE:
        buff.append("coalesce(");
        for (RFWField param : field.getFunctionParam()) {
          if (buff.length() > 9) buff.append(",");
          buff.append(evalRFWField(map, param, dialect));
        }
        buff.append(")");
        break;
      case CONCAT:
        buff.append("concat(");
        for (RFWField param : field.getFunctionParam()) {
          if (buff.length() > 7) buff.append(",");
          buff.append(evalRFWField(map, param, dialect));
        }
        buff.append(")");
        break;
      case CONSTANTE_STRING:
        buff.append("'").append(field.getConstantValue()).append("'");
        break;
      case CONSTANTE_NUMBER:
        buff.append(field.getConstantValue());
        break;
      case CONSTANT_NULL:
        buff.append("NULL");
        break;
      case MONTH:
        if (field.getField() != null) {
          buff.append("month(").append(evalFieldToColumn(map, field.getField(), dialect)).append(")");
        } else {
          buff.append("month(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(")");
        }
        break;
      case YEAR:
        if (field.getField() != null) {
          buff.append("year(").append(evalFieldToColumn(map, field.getField(), dialect)).append(")");
        } else {
          buff.append("year(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(")");
        }
        break;
      case MAXIMUM:
        buff.append("max(").append(evalFieldToColumn(map, field.getField(), dialect)).append(")");
        break;
      case MINIMUM:
        if (field.getField() != null) {
          buff.append("min(").append(evalFieldToColumn(map, field.getField(), dialect)).append(")");
        } else {
          buff.append("min(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(")");
        }
        break;
      case MULTIPLY:
        buff.append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append("*").append(evalRFWField(map, field.getFunctionParam().get(1), dialect));
        break;
      case DIVIDE:
        buff.append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append("/").append(evalRFWField(map, field.getFunctionParam().get(1), dialect));
        break;
      case WEEKDAY: // Deve retornar valores de 0 a 6, come�ando na segunda-feira.
        switch (dialect) {
          case DerbyDB:
            // Fun��o dayofweek do Derby difere da weekday definida no RFWDAO (leia defini��o da fun��o em RFWField), por isso o ajuste do valor retornado
            if (field.getField() != null) {
              buff.append("((dayofweek(").append(evalFieldToColumn(map, field.getField(), dialect)).append(") + 5) % 7)");
            } else {
              buff.append("((dayofweek(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(") + 5) % 7)");
            }
            break;
          case MySQL:
            if (field.getField() != null) {
              buff.append("weekday(").append(evalFieldToColumn(map, field.getField(), dialect)).append(")");
            } else {
              buff.append("weekday(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(")");
            }
            break;
        }
        break;
      case DAY:
        if (field.getField() != null) {
          buff.append("day(").append(evalFieldToColumn(map, field.getField(), dialect)).append(")");
        } else {
          buff.append("day(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(")");
        }
        break;
      case HOUR:
        if (field.getField() != null) {
          buff.append("hour(").append(evalFieldToColumn(map, field.getField(), dialect)).append(")");
        } else {
          buff.append("hour(").append(evalRFWField(map, field.getFunctionParam().get(0), dialect)).append(")");
        }
        break;
    }
    return buff.toString();

  }

  /**
   * Este m�todo recebe o objeto de mapeamento {@link DAOMap}, com as informa��es do mapeamento entre tabelas e objetos. E recebendo um field do VO (na estrutura do Math Object), � capaz de retornar a string como deve ser utilizada no SQL.<br>
   *
   * @param map Objeto com a defini��o do mapeamento entre tabelas/colunas e VO/atributos
   * @param field path do atributo do VO a ser convertido para o formato de tabela.coluna para o SQL.
   * @param dialect
   * @return Retorna uma string no formato ").append(dialect.getQM()).append("aliasTabela").append(dialect.getQM()).append(".").append(dialect.getQM()).append("coluna").append(dialect.getQM()).append(" que associada ao attributo do VO passado como par�metro.
   */
  private static String evalFieldToColumn(DAOMap map, String field, SQLDialect dialect) {
    final DAOMapField fMap = map.mapFieldByPath.get(field);
    StringBuilder buff = new StringBuilder();
    buff.append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM());
    return buff.toString();
  }

  /**
   * Cria o Statement SQL para inserir o objeto no banco de dados.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param path caminho at� o VO que est� sendo inserido no banco.
   * @param vo Objeto a ser inserido no banco de dados.
   * @param dialect
   *
   * @return PreparedStatemet pronto para realizar a opera��o no banco.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static <VO extends RFWVO> PreparedStatement createInsertStatement(Connection conn, DAOMap map, String path, VO vo, SQLDialect dialect) throws RFWException {
    return createInsertStatement(conn, map, path, vo, null, 0, dialect);
  }

  /**
   * Cria o Statement SQL para inserir o objeto no banco de dados, COM a cria��o da coluna de SortColumn. A sortcolumn � a coluna utilizada para salvar o �ndice de um objeto em uma lista e dessa forma garantir que a lista ter� a mesma ordem ao ser recuperada do banco de dados.<br>
   * � necess�rio um m�todo de cria��o especial pois a coluna de sort n�o deve fazer parte do objeto e � gerenciada pela classe do RFWDAO. Diferentemente da coluna da Hash que normalmente � algum atributo do pr�prio objeto.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param path caminho at� o VO que est� sendo inserido no banco.
   * @param vo Objeto a ser inserido no banco de dados.
   * @param sortColumn Nome da coluna onde � salvo o �ndice de ordem da Lista (se houver), ou Null caso n�o seja usado o recurso
   * @param sortIndex �ndice de indexa��o do item na lista.
   * @param dialect
   *
   * @return PreparedStatemet pronto para realizar a opera��o no banco.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static <VO extends RFWVO> PreparedStatement createInsertStatement(Connection conn, DAOMap map, String path, VO vo, String sortColumn, int sortIndex, SQLDialect dialect) throws RFWException {
    final StringBuffer sql = new StringBuffer();
    final LinkedList<Object> statementParameters = new LinkedList<>();
    try {
      final DAOMapTable mTable = map.getMapTableByPath(path);

      sql.append("INSERT INTO ").append(dialect.getQM()).append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" (");

      // Itera todos os campos em busca dos campos dessa tabela
      int c = 0;
      for (DAOMapField mField : map.getMapField()) {
        if (mField.table == mTable) {
          // Dependendo do dialeto n�o inclui as colunas de identifier, pois elas devem ser geradas sozinhas
          if ("ID".equals(mField.column.toUpperCase()) && dialect.getSkipInsertIDColumn()) continue;
          if (c > 0) sql.append(",");
          sql.append(dialect.getQM()).append(mField.column).append(dialect.getQM());
          c++;

          // Salvamos o valor do objeto na lista de atributos.
          Object value = RUReflex.getPropertyValue(vo, mField.field);

          // Verificamos se o atributo tem um converter, se tiver ele ser� usado para preparar os valores para o banco de dados
          if (!"id".equals(mField.field)) { // N�o aceita as annotations no campo ID
            final RFWDAOConverter convAnn = vo.getClass().getDeclaredField(mField.field).getAnnotation(RFWDAOConverter.class);
            if (convAnn != null) {
              final Object ni = convAnn.converterClass().newInstance();
              if (!(ni instanceof RFWDAOConverterInterface)) throw new RFWCriticalException("A classe '${0}' definida no atributo '${1}' da classe '${2}' n�o � um RFWDAOConverterInterface v�lido!", new String[] { convAnn.converterClass().getCanonicalName(), mField.field, vo.getClass().getCanonicalName() });
              value = ((RFWDAOConverterInterface) ni).toDB(value);
            } else {
              // Verificamos se o atributo for do tipo String e tem a annotation RFWMetaEncrypt, para encriptarmos o conte�do
              if (value != null && (value instanceof String)) {
                final RFWMetaEncrypt encAnn = vo.getClass().getDeclaredField(mField.field).getAnnotation(RFWMetaEncrypt.class);
                if (encAnn != null) {
                  value = RUEncrypter.encryptDES((String) value, encAnn.key());
                }
              }
            }
          }
          statementParameters.add(value);
        }
      }

      // Se tiver uma coluna de Sort, inclu�mos ela agora no final dos campos
      if (sortColumn != null) {
        sql.append(",").append(dialect.getQM()).append(sortColumn).append(dialect.getQM());
        statementParameters.add(new Integer(sortIndex));
      }

      sql.append(") VALUES (?");
      for (int i = 1; i < statementParameters.size(); i++) {
        sql.append(",?");
      }
      sql.append(")");

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s, Statement.RETURN_GENERATED_KEYS);
      writeStatementParameters(stmt, statementParameters);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Cria o Statement SQL para inserir o dado de de um atributo anotado com {@link RFWMetaCollectionField} no banco de dados.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param path caminho at� o VO que est� sendo inserido no banco.
   * @param items Lista de Item da Collection. Podendo ser um objeto qualquer em caso de List, ou uma List de Entry (que cont�m a chave e valor) no caso de Map.
   * @param parentID ID do objeto pai para popular a coluna de FK.
   * @param dialect
   *
   * @return PreparedStatemet pronto para realizar a opera��o no banco.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static <VO extends RFWVO> PreparedStatement createInsertCollectionStatement(Connection conn, DAOMap map, String path, List<?> items, Long parentID, SQLDialect dialect, RFWMetaCollectionField ann) throws RFWException {
    final StringBuffer sql = new StringBuffer();
    final LinkedList<Object> statementParameters = new LinkedList<>();
    try {
      final DAOMapTable mTable = map.getMapTableByPath(path);

      sql.append("INSERT INTO ").append(dialect.getQM()).append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" (");

      // Itera todos os campos em busca dos campos dessa tabela
      int c = 0;
      for (DAOMapField mField : map.getMapField()) {
        if (mField.table == mTable) {
          if (c > 0) sql.append(",");
          sql.append(dialect.getQM()).append(mField.column).append(dialect.getQM());
          c++;
        }
      }

      sql.append(") VALUES ");

      int i = 0;
      for (Object item : items) {
        if (i > 0) sql.append(",");
        sql.append("(");

        c = 0;
        for (DAOMapField mField : map.getMapField()) { // Reinitera as colunas para que fique na mesma ordem que a itera��o acima que escreveu o nome das colunas
          if (mField.table == mTable) {
            if (c > 0) sql.append(",");
            sql.append("?");

            Object value = null;
            // Verificamos o sufixo do path da coluna mapeada...
            if (mField.field.endsWith("@fk")) { // ...Indica que � a coluna onde salvamos o ID do objeto pai
              value = parentID;
            } else if (mField.field.endsWith("@keyColumn")) { // ...Indica que � a coluna onde salvamos a chave da Hash
              if (!(item instanceof Entry<?, ?>)) {
                throw new RFWCriticalException("Encontrado um atributo com keyColumn definido, mas o objeto recebido para persistir n�o � um Entry<?, ?>.", new String[] { path, item.getClass().getCanonicalName() });
              }
              value = ((Entry<?, ?>) item).getKey();

              if (RFWDAOConverterInterface.class.isAssignableFrom(ann.keyConverterClass())) {
                Object ni = ann.keyConverterClass().newInstance();
                if (!(ni instanceof RFWDAOConverterInterface)) throw new RFWCriticalException("A classe '${0}' definida no atributo '${1}' da classe '${2}' n�o � um RFWDAOConverterInterface v�lido!", new String[] { ann.keyConverterClass().getCanonicalName(), mField.field, mField.table.type.getCanonicalName() });
                value = ((RFWDAOConverterInterface) ni).toDB(value);
              }

            } else if (mField.field.endsWith("@sortColumn")) { // ...Indica que � a coluna onde salvamos o �ndice de ordem do objeto
              value = items.indexOf(item);
            } else { // ..Se n�o � nenhuma das anteriores, � a coluna onde salvamos o conte�do do objeto
              value = item; // Em caso de lista o valor a ser salvo j� � o item...
              if (item instanceof Entry<?, ?>) { // ...Mas caso seja um Entry de um Map, temos de trocar pelo valor da hash
                value = ((Entry<?, ?>) item).getValue();
              }
            }

            // Salvamos o valor do objeto na lista de atributos.
            statementParameters.add(value);

            c++;
          }
        }
        sql.append(")");
        i++;
      }

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s, Statement.RETURN_GENERATED_KEYS);
      writeStatementParameters(stmt, statementParameters);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Cria o Statement SQL para atualizar o objeto no banco de dados.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param path caminho at� o VO que est� sendo inserido no banco.
   * @param vo Objeto a ser inserido no banco de dados.
   * @param dialect
   *
   * @return PreparedStatemet pronto para realizar a opera��o no banco.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static <VO extends RFWVO> PreparedStatement createUpdateStatement(Connection conn, DAOMap map, String path, VO vo, SQLDialect dialect) throws RFWException {
    return createUpdateStatement(conn, map, path, vo, null, 0, dialect);
  }

  /**
   * Cria o Statement SQL para atualizar o objeto no banco de dados, COM a atualiza��o da coluna de SortColumn. A sortcolumn � a coluna utilizada para salvar o �ndice de um objeto em uma lista e dessa forma garantir que a lista ter� a mesma ordem ao ser recuperada do banco de dados.<br>
   * � necess�rio um m�todo de cria��o especial pois a coluna de sort n�o deve fazer parte do objeto e � gerenciada pela classe do RFWDAO. Diferentemente da coluna da Hash que normalmente � algum atributo do pr�prio objeto.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param path caminho at� o VO que est� sendo inserido no banco.
   * @param vo Objeto a ser inserido no banco de dados.
   * @param sortColumn Nome da coluna onde � salvo o �ndice de ordem da Lista (se houver), ou Null caso n�o seja usado o recurso
   * @param sortIndex �ndice de indexa��o do item na lista.
   * @param dialect
   *
   * @return PreparedStatemet pronto para realizar a opera��o no banco.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static <VO extends RFWVO> PreparedStatement createUpdateStatement(Connection conn, DAOMap map, String path, VO vo, String sortColumn, int sortIndex, SQLDialect dialect) throws RFWException {
    final StringBuffer sql = new StringBuffer();
    final LinkedList<Object> statementParameters = new LinkedList<>();
    try {
      final DAOMapTable mTable = map.getMapTableByPath(path);

      sql.append("UPDATE ").append(dialect.getQM()).append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" SET ");

      // Itera todos os campos em busca dos campos dessa tabela
      int c = 0;
      for (DAOMapField mField : map.getMapField()) {
        if (mField.table == mTable) {
          if (c > 0) sql.append(",");
          if (!"id".equals(mField.column)) { // Pula a coluna ID para que ela n�o entre no corpo do UPDATE, s� no WHERE abaixo.
            sql.append(dialect.getQM()).append(mField.column).append(dialect.getQM()).append("=?");
            c++;

            // Salvamos o valor do objeto na lista de atributos.
            Object value = RUReflex.getPropertyValue(vo, mField.field);

            // Verificamos se o atributo tem um converter, se tiver ele ser� usado para preparar os valores para o banco de dados
            if (!"id".equals(mField.field)) { // N�o acieta as annotations no campo ID
              final RFWDAOConverter convAnn = vo.getClass().getDeclaredField(mField.field).getAnnotation(RFWDAOConverter.class);
              if (convAnn != null) {
                final Object ni = convAnn.converterClass().newInstance();
                if (!(ni instanceof RFWDAOConverterInterface)) throw new RFWCriticalException("A classe '${0}' definida no atributo '${1}' da classe '${2}' n�o � um RFWDAOConverterInterface v�lido!", new String[] { convAnn.converterClass().getCanonicalName(), mField.field, vo.getClass().getCanonicalName() });
                value = ((RFWDAOConverterInterface) ni).toDB(value);
              } else {
                // Verificamos se o atributo for do tipo String e tem a annotation RFWMetaEncrypt, para encriptarmos o conte�do
                if (value != null && (value instanceof String)) {
                  final RFWMetaEncrypt encAnn = vo.getClass().getDeclaredField(mField.field).getAnnotation(RFWMetaEncrypt.class);
                  if (encAnn != null) {
                    value = RUEncrypter.encryptDES((String) value, encAnn.key());
                  }
                }
              }
            }
            statementParameters.add(value);
          }
        }
      }

      // Se tiver uma coluna de Sort, inclu�mos ela agora no final dos campos
      if (sortColumn != null) {
        sql.append(",").append(dialect.getQM()).append(sortColumn).append(dialect.getQM()).append("=?");
        statementParameters.add(new Integer(sortIndex));
      }

      sql.append(" WHERE ").append(dialect.getQM()).append("id").append(dialect.getQM()).append("=?");
      statementParameters.add(vo.getId());

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s);
      writeStatementParameters(stmt, statementParameters);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Cria o Statement para atualiza v�rios objetos de uma �nica vez no banco de dados.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param setValues Conjunto de valores a serem atualizados. Onde a chave do SET � o campo do VO. Geralmente gerado a partir dos Meta Objetos, e o conte�do o valor a ser atribuido no campo.
   * @param mo MatchObject para filtrar os objetos que ser�o atualizados.
   * @param voClass Classe do objeto principal.
   * @param dialect
   *
   * @return PreparedStatemet pronto para realizar a opera��o no banco.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static <VO extends RFWVO> PreparedStatement createMassUpdateStatement(Connection conn, DAOMap map, Map<String, Object> setValues, RFWMO mo, Class<? extends RFWVO> voClass, SQLDialect dialect) throws RFWException {
    final StringBuffer sql = new StringBuffer();

    final LinkedList<Object> parameters = new LinkedList<>();

    try {
      // UPDATE employees
      // LEFT JOIN merits ON employees.performance = merits.performance
      // SET salary = salary + salary * 0.015
      // WHERE merits.percentage IS NULL;

      // ==> UPDATE
      {
        // Come�a incluindo o mapeamento raiz
        final DAOMapTable mTableRoot = map.mapTableByPath.get("");
        sql.append("UPDATE ").append(dialect.getQM()).append(mTableRoot.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTableRoot.table).append(dialect.getQM()).append(" AS ").append(dialect.getQM()).append(mTableRoot.alias).append(dialect.getQM());
        // Itera as demais tabelas como Left Join
        for (DAOMapTable mTable : map.mapTableByPath.values()) {
          // Evita a tabela raiz, j� que ela j� foi adicionada
          if (!"".equals(mTable.path)) {
            sql.append(" LEFT JOIN ").append(dialect.getQM()).append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" AS ").append(dialect.getQM()).append(mTable.alias).append(dialect.getQM()).append(" ON ").append(dialect.getQM()).append(mTable.joinAlias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.joinColumn).append(dialect.getQM()).append(" = ").append(dialect.getQM()).append(mTable.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.column).append(dialect.getQM());
          }
        }
      }

      // Itera todos os campos que ser�o atualizados
      sql.append(" SET ");
      int c = 0;
      for (String column : setValues.keySet()) {
        if ("id".equals(column)) throw new RFWCriticalException("N�o � permitido atualizar a coluna ID no MassUpdate do RFWDAO!");
        DAOMapField mField = map.getMapFieldByPath(column);

        if (c > 0) sql.append(",");
        sql.append(dialect.getQM()).append(mField.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mField.column).append(dialect.getQM()).append("=?");

        // Salvamos o valor da coluna na lista de atributos.
        Object value = setValues.get(column);

        // Verificamos se o atributo tem um converter, se tiver ele ser� usado para preparar os valores para o banco de dados
        final RFWDAOConverter convAnn = voClass.getDeclaredField(mField.field).getAnnotation(RFWDAOConverter.class);
        if (convAnn != null) {
          final Object ni = convAnn.converterClass().newInstance();
          if (!(ni instanceof RFWDAOConverterInterface)) throw new RFWCriticalException("A classe '${0}' definida no atributo '${1}' da classe '${2}' n�o � um RFWDAOConverterInterface v�lido!", new String[] { convAnn.converterClass().getCanonicalName(), mField.field, voClass.getCanonicalName() });
          value = ((RFWDAOConverterInterface) ni).toDB(value);
        } else {
          // Verificamos se o atributo for do tipo String e tem a annotation RFWMetaEncrypt, para encriptarmos o conte�do
          if (value != null && (value instanceof String)) {
            final RFWMetaEncrypt encAnn = voClass.getDeclaredField(mField.field).getAnnotation(RFWMetaEncrypt.class);
            if (encAnn != null) {
              value = RUEncrypter.encryptDES((String) value, encAnn.key());
            }
          }
        }
        parameters.add(value);
        c++;
      }

      // ===> WHERE
      if (mo != null && mo.size() > 0) {
        final StringBuilder where = writeWhere(map, mo, parameters, dialect);
        // As vezes temos MO, mas ele n�o gera nenhuma consulta (um MO em branco), neste caso n�o escrevemos o "WHERE" ou teremos um SQL inv�lido
        if (where.length() > 0) sql.append(" WHERE").append(where);
      } else {
        throw new RFWCriticalException("O RFWDAO MassUpdate n�o aceita uma atualiza��o com MO nulo ou sem parametro nenhum por seguran�a contra cagadas. Se sua inten��o for mesmo atualizar todo o banco, crie uma condi��o como \"id > -1\" para passar por esta valida��o!");
      }

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s);
      writeStatementParameters(stmt, parameters);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Cria o Statement SQL para inserir o mapeamento entre dois objetos em uma tabela de joinAlias.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param path Caminho at� o VO do objeto que est� sendo associado no relacionamento N:N no banco.
   * @param vo1 VO que estamos persistindo presente no mapeamento.
   * @param vo2 VO do relacionamento ManyToMany que ser� associado.
   * @param dialect
   *
   * @return PreparedStatemet pronto para realizar a opera��o no banco.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static <VO extends RFWVO> PreparedStatement createManyToManyInsertStatement(Connection conn, DAOMap map, String path, VO vo1, VO vo2, SQLDialect dialect) throws RFWException {
    final StringBuffer sql = new StringBuffer();
    final LinkedList<Object> statementParameters = new LinkedList<>();
    try {
      final DAOMapTable jTable = map.getMapTableByPath("." + path); // Obtem o mapeamento da tabela de joinAlias, que � colocada na Hash com mesmo caminho do path com o prefixo de "."
      final DAOMapTable mTable = map.getMapTableByPath(path);

      sql.append("INSERT INTO ").append(dialect.getQM()).append(jTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(jTable.table);

      sql.append(dialect.getQM()).append(" (").append(dialect.getQM()).append(jTable.column).append(dialect.getQM()).append(", ").append(dialect.getQM()).append(mTable.joinColumn);

      sql.append(dialect.getQM()).append(") VALUES (?,?)");

      statementParameters.add(vo1.getId());
      statementParameters.add(vo2.getId());

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s);
      writeStatementParameters(stmt, statementParameters);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Cria o Statement SQL para consutlar um mapeamento entre dois objetos em uma tabela de joinAlias.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param path Caminho at� o VO do objeto que est� sendo associado no relacionamento N:N no banco.
   * @param vo1 VO que estamos persistindo presente no mapeamento.
   * @param vo2 VO do relacionamento ManyToMany que ser� associado.
   * @param dialect
   *
   * @return PreparedStatemet pronto para realizar a opera��o no banco.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static <VO extends RFWVO> PreparedStatement createManyToManySelectStatement(Connection conn, DAOMap map, String path, VO vo1, VO vo2, SQLDialect dialect) throws RFWException {
    final StringBuffer sql = new StringBuffer();
    final LinkedList<Object> statementParameters = new LinkedList<>();
    try {
      final DAOMapTable jTable = map.getMapTableByPath("." + path); // Obtem o mapeamento da tabela de joinAlias, que � colocada na Hash com mesmo caminho do path com o prefixo de "."
      final DAOMapTable mTable = map.getMapTableByPath(path);

      sql.append("SELECT * FROM ").append(dialect.getQM()).append(jTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(jTable.table).append(dialect.getQM()).append(" WHERE ").append(dialect.getQM()).append(jTable.column).append(dialect.getQM()).append("=? AND ").append(dialect.getQM()).append(mTable.joinColumn).append(dialect.getQM()).append("=?");

      statementParameters.add(vo1.getId());
      statementParameters.add(vo2.getId());

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s);
      writeStatementParameters(stmt, statementParameters);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Cria o Statement SQL para excluir um mapeamento entre dois objetos em uma tabela de joinAlias.
   *
   * @param conn Conex�o com o banco para cria��o do PreparedStatement.
   * @param map Mapeamento do VO com as tabelas do banco.
   * @param path Caminho at� o VO do objeto que est� sendo associado no relacionamento N:N no banco.
   * @param vo1 VO que estamos persistindo presente no mapeamento.
   * @param vo2 VO do relacionamento ManyToMany que ser� associado.
   * @param dialect
   *
   * @return PreparedStatemet pronto para realizar a opera��o no banco.
   * @throws RFWException Lan�ado em caso de Erro.
   */
  public static <VO extends RFWVO> PreparedStatement createManyToManyDeleteStatement(Connection conn, DAOMap map, String path, VO vo1, VO vo2, SQLDialect dialect) throws RFWException {
    final StringBuffer sql = new StringBuffer();
    final LinkedList<Object> statementParameters = new LinkedList<>();
    try {
      final DAOMapTable jTable = map.getMapTableByPath("." + path); // Obtem o mapeamento da tabela de joinAlias, que � colocada na Hash com mesmo caminho do path com o prefixo de "."
      final DAOMapTable mTable = map.getMapTableByPath(path);

      sql.append("DELETE FROM ").append(dialect.getQM()).append(jTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(jTable.table).append(dialect.getQM()).append(" WHERE ").append(dialect.getQM()).append(jTable.column).append(dialect.getQM()).append("=? AND ").append(dialect.getQM()).append(mTable.joinColumn).append(dialect.getQM()).append("=?");

      statementParameters.add(vo1.getId());
      statementParameters.add(vo2.getId());

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s);
      writeStatementParameters(stmt, statementParameters);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  public static <VO extends RFWVO> PreparedStatement createDeleteStatement(Connection conn, DAOMap map, String path, SQLDialect dialect, Long... ids) throws RFWException {
    final StringBuffer sql = new StringBuffer();
    try {
      final DAOMapTable mTable = map.getMapTableByPath(path);

      sql.append("DELETE FROM ").append(dialect.getQM()).append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" WHERE ").append(dialect.getQM()).append("id").append(dialect.getQM()).append(" IN (");
      for (int i = 0; i < ids.length; i++) {
        if (i > 0) sql.append(",");
        sql.append("?");
      }
      sql.append(")");

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s);
      for (int i = 0; i < ids.length; i++) {
        stmt.setLong(i + 1, ids[i]);
      }
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Exclui todos os elementos de uma {@link RFWMetaCollectionField} associado a um objeto.
   *
   * @param conn Conex�o com o banco de dados.
   * @param map Mapeamento do Banco de dados.
   * @param path Caminho da tabela onde est�o os elementos. No Mapeamento o caminho � criado com o caminho do atributo prexecido de um ".".
   * @param id
   * @param dialect
   * @return Statement preparado para ser executado no Banco de dados.
   * @throws RFWException
   */
  public static <VO extends RFWVO> PreparedStatement createDeleteCollectionStatement(Connection conn, DAOMap map, String path, Long id, SQLDialect dialect) throws RFWException {
    final StringBuffer sql = new StringBuffer();
    try {
      final DAOMapTable mTable = map.getMapTableByPath(path);

      sql.append("DELETE FROM ").append(dialect.getQM()).append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" WHERE ").append(dialect.getQM()).append(mTable.column).append(dialect.getQM()).append("=?");

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s);
      stmt.setLong(1, id);
      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Este m�todo adiciona um RFWMO para as condi��es da clausula Where do SQL.
   *
   * @param map Descritor dos mapeamentos entre Objeto de Tabelas do Banco da dados.
   * @param mo MO com as condi��es a serem usadas no Where
   * @param statementParameters Lista onde ser�o armazenados os objetos para a posteriro troca dos '?' pelos valores
   * @param dialect
   * @throws RFWException
   */
  @SuppressWarnings("unchecked")
  private static StringBuilder writeWhere(DAOMap map, RFWMO mo, LinkedList<Object> statementParameters, SQLDialect dialect) throws RFWException {
    final StringBuilder buff = new StringBuilder();

    // ==> Equals
    for (RFWMOData data : mo.getEqual()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final
      RFWMetaEncrypt encAnn = null;
      if (fMap != null) encAnn = RUReflex.getRFWDAOEncryptAnnotation(fMap.table.type, fMap.field.replaceAll("\\@", ""));
      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" = ?");
      if (encAnn != null) {
        statementParameters.add(RUEncrypter.encryptDES((String) data.getValue(), encAnn.key()));
      } else {
        statementParameters.add(data.getValue());
      }
    }

    // ==> Not Equals
    for (RFWMOData data : mo.getNotEqual()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final
      RFWMetaEncrypt encAnn = null;
      if (fMap != null) encAnn = RUReflex.getRFWDAOEncryptAnnotation(fMap.table.type, fMap.field.replaceAll("\\@", ""));
      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" != ?");
      if (encAnn != null) {
        statementParameters.add(RUEncrypter.encryptDES((String) data.getValue(), encAnn.key()));
      } else {
        statementParameters.add(data.getValue());
      }
    }

    // ==> Like
    for (RFWMOData data : mo.getLike()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final
      RFWMetaEncrypt encAnn = null;
      if (fMap != null) encAnn = RUReflex.getRFWDAOEncryptAnnotation(fMap.table.type, fMap.field.replaceAll("\\@", ""));
      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" like ?");
      if (encAnn != null) {
        // statementParameters.add(RUEncrypter.encryptDES((String) data.getValue(), encAnn.key()));
        throw new RFWCriticalException("N�o � poss�vel utilizar o operador 'like' em atributos que utilizam o valor criptografado no banco de dados. (${0}.${1}.${2})", new String[] { fMap.table.schema, fMap.table.table, fMap.column });
      } else {
        statementParameters.add(data.getValue());
      }
    }

    // ==> GreaterThenOrEqualTo
    for (RFWMOData data : mo.getGreaterThanOrEqualTo()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final
      RFWMetaEncrypt encAnn = null;
      if (fMap != null) encAnn = RUReflex.getRFWDAOEncryptAnnotation(fMap.table.type, fMap.field.replaceAll("\\@", ""));
      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" >= ?");
      if (encAnn != null) {
        throw new RFWCriticalException("N�o � poss�vel utilizar o operador 'like' em atributos que utilizam o valor criptografado no banco de dados. (${0}.${1}.${2})", new String[] { fMap.table.schema, fMap.table.table, fMap.column });
      } else {
        statementParameters.add(data.getValue());
      }
    }

    // ==> GreaterThen
    for (RFWMOData data : mo.getGreaterThan()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final
      RFWMetaEncrypt encAnn = null;
      if (fMap != null) encAnn = RUReflex.getRFWDAOEncryptAnnotation(fMap.table.type, fMap.field.replaceAll("\\@", ""));
      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" > ?");
      if (encAnn != null) {
        throw new RFWCriticalException("N�o � poss�vel utilizar o operador 'like' em atributos que utilizam o valor criptografado no banco de dados. (${0}.${1}.${2})", new String[] { fMap.table.schema, fMap.table.table, fMap.column });
      } else {
        statementParameters.add(data.getValue());
      }
    }

    // ==> LessThenOrEqualTo
    for (RFWMOData data : mo.getLessThanOrEqualTo()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final
      RFWMetaEncrypt encAnn = null;
      if (fMap != null) encAnn = RUReflex.getRFWDAOEncryptAnnotation(fMap.table.type, fMap.field.replaceAll("\\@", ""));
      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" <= ?");
      if (encAnn != null) {
        throw new RFWCriticalException("N�o � poss�vel utilizar o operador 'like' em atributos que utilizam o valor criptografado no banco de dados. (${0}.${1}.${2})", new String[] { fMap.table.schema, fMap.table.table, fMap.column });
      } else {
        statementParameters.add(data.getValue());
      }
    }

    // ==> LessThen
    for (RFWMOData data : mo.getLessThan()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final
      RFWMetaEncrypt encAnn = null;
      if (fMap != null) encAnn = RUReflex.getRFWDAOEncryptAnnotation(fMap.table.type, fMap.field.replaceAll("\\@", ""));
      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" < ?");
      if (encAnn != null) {
        throw new RFWCriticalException("N�o � poss�vel utilizar o operador 'like' em atributos que utilizam o valor criptografado no banco de dados. (${0}.${1}.${2})", new String[] { fMap.table.schema, fMap.table.table, fMap.column });
      } else {
        statementParameters.add(data.getValue());
      }
    }

    // ==> IsNull
    for (RFWMOData data : mo.getIsNull()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final

      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" IS NULL");
    }

    // ==> IsNotNull
    for (RFWMOData data : mo.getIsNotNull()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final

      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" IS NOT NULL");
    }

    // ==> In
    for (RFWMOData data : mo.getIn()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final
      RFWMetaEncrypt encAnn = null;
      if (fMap != null) encAnn = RUReflex.getRFWDAOEncryptAnnotation(fMap.table.type, fMap.field.replaceAll("\\@", ""));
      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" IN (");
      int c = 0;
      for (Object value : (Collection<Object>) data.getValue()) {
        if (c > 0) {
          buff.append(",?");
        } else {
          buff.append("?");
        }
        c++;
        if (encAnn != null) {
          statementParameters.add(RUEncrypter.encryptDES((String) value, encAnn.key()));
        } else {
          statementParameters.add(value);
        }
      }
      buff.append(")");
    }

    // ==> NotIn
    for (RFWMOData data : mo.getNotIn()) {
      DAOMapField fMap = map.mapFieldByPath.get(data.getFieldname());
      if (fMap == null) fMap = map.mapFieldByPath.get(data.getFieldname() + "@"); // se n�o encontrou, pode ser que o atributo seja uma Collection, tentamos encontrar com o '@' no final
      RFWMetaEncrypt encAnn = null;
      if (fMap != null) encAnn = RUReflex.getRFWDAOEncryptAnnotation(fMap.table.type, fMap.field.replaceAll("\\@", ""));
      if (buff.length() > 0) {
        switch (mo.getAppendmethod()) {
          case AND:
            buff.append(" AND");
            break;
          case OR:
            buff.append(" OR");
            break;
        }
      }
      buff.append(" ").append(dialect.getQM()).append(fMap.table.alias).append(dialect.getQM()).append(".").append(dialect.getQM()).append(fMap.column).append(dialect.getQM()).append(" NOT IN (");
      int c = 0;
      for (Object value : (Collection<Object>) data.getValue()) {
        if (c > 0) {
          buff.append(",?");
        } else {
          buff.append("?");
        }
        c++;
        if (encAnn != null) {
          statementParameters.add(RUEncrypter.encryptDES((String) value, encAnn.key()));
        } else {
          statementParameters.add(value);
        }
      }
      buff.append(")");
    }

    // Fazemos agora o SubMO recursivamente
    for (RFWMO subMO : mo.getSubmo()) {
      final StringBuilder subWhere = writeWhere(map, subMO, statementParameters, dialect);
      // O IF nos protege de termos um SUBMO vazio, assim n�o terminamos com uma SQL inv�lida.
      if (subWhere.length() > 0) {
        if (buff.length() > 0) {
          switch (mo.getAppendmethod()) {
            case AND:
              buff.append(" AND");
              break;
            case OR:
              buff.append(" OR");
              break;
          }
        }
        buff.append(" (").append(subWhere).append(")");
      }
    }

    return buff;
  }

  /**
   * Escreve os objetos/parametros criados no Statement
   *
   * @param statementParameters
   * @param map
   *
   * @throws RFWException
   */
  private static void writeStatementParameters(PreparedStatement stmt, LinkedList<Object> statementParameters) throws RFWException {
    try {
      int i = 1;
      for (Object o : statementParameters) {
        if (o == null) {
          stmt.setNull(i, Types.VARCHAR);
        } else if (o instanceof Long) {
          stmt.setLong(i, (Long) o);
        } else if (o instanceof String) {
          stmt.setString(i, (String) o);
        } else if (o instanceof Date) {
          stmt.setTimestamp(i, new Timestamp(((Date) o).getTime()));
        } else if (o instanceof LocalDate) {
          // stmt.setObject(i, o, Types.DATE);
          stmt.setDate(i, java.sql.Date.valueOf((LocalDate) o));
        } else if (o instanceof LocalTime) {
          // stmt.setObject(i, o, Types.TIME);
          stmt.setTime(i, Time.valueOf((LocalTime) o));
        } else if (o instanceof LocalDateTime) {
          // stmt.setObject(i, o, Types.TIMESTAMP);
          stmt.setTimestamp(i, Timestamp.valueOf((LocalDateTime) o));
        } else if (o instanceof Integer) {
          stmt.setInt(i, (Integer) o);
        } else if (o instanceof Boolean) {
          stmt.setBoolean(i, (Boolean) o);
        } else if (o instanceof Enum<?>) {
          stmt.setString(i, ((Enum<?>) o).name());
        } else if (o instanceof BigDecimal) {
          stmt.setBigDecimal(i, (BigDecimal) o);
        } else if (o instanceof Double) {
          stmt.setDouble(i, (Double) o);
        } else if (o instanceof Float) {
          stmt.setFloat(i, (Float) o);
        } else if (o instanceof Short) {
          stmt.setShort(i, (Short) o);
        } else if (o instanceof Character) {
          stmt.setString(i, String.valueOf(o));
        } else if (o instanceof Byte) {
          stmt.setByte(i, (Byte) o);
        } else if (o instanceof byte[]) {
          byte[] b = (byte[]) o;
          try (ByteArrayInputStream is = new ByteArrayInputStream(b)) {
            stmt.setBlob(i, is);
          }
        } else if (RFWVO.class.isAssignableFrom(o.getClass())) {
          // Sempre que � um RFWVO na verdade temos de definir seu ID, pois � o que associa o objeto � coluna
          stmt.setLong(i, ((RFWVO) o).getId());
        } else {
          throw new RFWCriticalException("RFW_ERR_000012", new String[] { o.getClass().getCanonicalName() });
        }
        i++;
      }
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000008", e);
    }
  }

  /**
   * Cria um statement para atualizar a coluna de FK quando a coluna com a FK est� no cadastro da contraparte.
   *
   * @param conn Conex�o
   * @param map Mapeamento Objeto x Tabelas.
   * @param path Caminho para o objeto que ter� a associa��o substituida.
   * @param id ID do objeto que ter� sua FK atualizada.
   * @param newId Valor com o novo ID da coluna de FK. Pode ser NULL caso a associa��o esteja sendo desfeita.
   * @param dialect
   * @return Statement preparado para ser executado no Banco de dados.
   * @throws RFWCriticalException
   */
  public static PreparedStatement createUpdateExternalFKStatement(Connection conn, DAOMap map, String path, Long id, Long newId, SQLDialect dialect) throws RFWCriticalException {
    final StringBuffer sql = new StringBuffer();
    try {
      final DAOMapTable mTable = map.getMapTableByPath(path);

      sql.append("UPDATE ").append(dialect.getQM()).append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" SET ").append(dialect.getQM()).append(mTable.column).append(dialect.getQM()).append("=? WHERE ").append(dialect.getQM()).append("id").append(dialect.getQM()).append("=?");

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s);
      if (newId != null) {
        stmt.setLong(1, newId);
      } else {
        stmt.setNull(1, Types.BIGINT);
      }
      stmt.setLong(2, id);

      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Cria um statement para atualizar a coluna de FK do pr�prio objeto (sem alterar mais nada). Utilizado quando deixamos o objeto para atualizar a FK posteriemente (casos do INNER_ASSOCIATION).
   *
   * @param conn Conex�o
   * @param map Mapeamento Objeto x Tabelas.
   * @param path Caminho completo at� o objeto que ser� atualizado.
   * @param property Propriedade do objeto que tem a associa��o com a FK na pr�pria tabela.
   * @param id ID do objeto que ter� sua FK atualizada.
   * @param newId Valor com o novo ID da coluna de FK. Pode ser NULL caso a associa��o esteja sendo desfeita.
   * @param dialect
   * @return Statement preparado para ser executado no Banco de dados.
   * @throws RFWCriticalException
   */
  public static PreparedStatement createUpdateInternalFKStatement(Connection conn, DAOMap map, String path, String property, Long id, Long newId, SQLDialect dialect) throws RFWCriticalException {
    final StringBuffer sql = new StringBuffer();
    try {
      final DAOMapField mField = map.getMapFieldByPath(path, property);
      final DAOMapTable mTable = mField.table;

      sql.append("UPDATE ").append(dialect.getQM()).append(mTable.schema).append(dialect.getQM()).append(".").append(dialect.getQM()).append(mTable.table).append(dialect.getQM()).append(" SET ").append(dialect.getQM()).append(mField.column).append(dialect.getQM()).append("=? WHERE ").append(dialect.getQM()).append("id").append(dialect.getQM()).append("=?");

      final String s = sql.toString();
      // ATEN��O: N�O USAR O RFWLOGGER, OU TERMINAR EM LOOP INFINITO. Gerar Log ao gravar os Log n�o d� certo!!!!
      if (RFW.isDevelopmentEnvironment()) System.out.println(s);
      PreparedStatement stmt = conn.prepareStatement(s);
      if (newId != null) {
        stmt.setLong(1, newId);
      } else {
        stmt.setNull(1, Types.BIGINT);
      }
      stmt.setLong(2, id);

      return stmt;
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_ERR_000010", new String[] { sql.toString() }, e);
    }
  }

  /**
   * Este m�todo cria um DAOMap a partir de um map completo. Utilizado para obter apenas parte da estrutura sem ter que reler toda a cadeia de RFWVO.<Br>
   * O funcionamento deste m�todo segue as seguintes regras:<br>
   * <li>Recebe uma {@link DAOMapTable} para indicar a partir de que ponto queremos a estrutura.
   * <li>Todas as tabelas de joins feitos a essa tabela, ou tabelas subsequ�ntes ser�o mantidos, todos os demais removidos.
   * <li>Os Alias das tabelas continuaram sendo os mesmos (n�o voltar� a contagem sequ�ncial). Assim, deixaremos de ter qualquer mapeamento com alias t0, uma vez que alias t0 � a raiz e qualquer subMap n�o o incluir�.
   * <li>A tabela passada como startTable automaticamente passar� a ser a tabela raiz, e por tanto n�o ter� mais a infoma��o de "joinTable".
   * <li>Todos os "Paths" ser�o corrigidos. A nova tabela raiz passar� a ter o path "", e as demais ter�o o caminho cortado, conforme o caminho inicial da tabela raiz. Deixando todos os caminhos relativos a nova tabela raiz.
   *
   * @param startTable Tabela Inicial da Nova Estrutura de Mapeamento.
   * @return Nova estrutura de mapeamento a partir de determinado ponto da estrutura.
   * @throws RFWException
   */
  public DAOMap createSubMap(DAOMapTable startTable) throws RFWException {
    final DAOMap newMap = new DAOMap();

    if ("".equals(startTable.path)) throw new RFWCriticalException("N�o � permitido criar uma substrutura da pr�pria tabela raiz!");

    final int cutPath = startTable.path.length() + 1;

    // Clona e adiciona a tabela como tabela raiz.
    newMap.createMapTable(startTable.type, "", startTable.schema, startTable.table, null, null, null, startTable.alias);

    int size = -1;
    while (size != newMap.mapTableByAlias.size()) {
      size = newMap.mapTableByAlias.size();

      // Itera as demais tabelas para o Join
      for (DAOMapTable mTable : this.mapTableByPath.values()) {
        if (!newMap.mapTableByAlias.containsKey(mTable.alias) && newMap.mapTableByAlias.containsKey(mTable.joinAlias)) {
          newMap.createMapTable(mTable.type, mTable.path.substring(cutPath), mTable.schema, mTable.table, mTable.column, mTable.joinAlias, mTable.joinColumn, mTable.alias);
        }
      }
    }

    // Agora que criamos todas as novas tabelas, vamos copiar oe mapeamentos das colunas
    for (DAOMapField mField : this.getMapField()) {
      DAOMapTable mTable = newMap.getMapTableByAlias(mField.table.alias);
      if (mTable != null) {
        String path = "";
        if (cutPath < mField.path.length()) path = mField.path.substring(cutPath);
        newMap.createMapField(path, mField.field, mTable, mField.column);
      }
    }

    return newMap;
  }

  public DAOMapTable getRootTable() {
    return rootTable;
  }

}
