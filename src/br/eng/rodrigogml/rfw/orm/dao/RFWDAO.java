package br.eng.rodrigogml.rfw.orm.dao;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import br.eng.rodrigogml.rfw.kernel.RFW;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWCriticalException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWValidationException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWWarningException;
import br.eng.rodrigogml.rfw.kernel.preprocess.PreProcess;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaCollectionField;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaEncrypt;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaRelationshipField;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaRelationshipField.RelationshipTypes;
import br.eng.rodrigogml.rfw.kernel.utils.RUArray;
import br.eng.rodrigogml.rfw.kernel.utils.RUEncrypter;
import br.eng.rodrigogml.rfw.kernel.utils.RUFile;
import br.eng.rodrigogml.rfw.kernel.utils.RUReflex;
import br.eng.rodrigogml.rfw.kernel.utils.RUString;
import br.eng.rodrigogml.rfw.kernel.vo.RFWField;
import br.eng.rodrigogml.rfw.kernel.vo.RFWMO;
import br.eng.rodrigogml.rfw.kernel.vo.RFWOrderBy;
import br.eng.rodrigogml.rfw.kernel.vo.RFWVO;
import br.eng.rodrigogml.rfw.orm.dao.DAOMap.DAOMapField;
import br.eng.rodrigogml.rfw.orm.dao.DAOMap.DAOMapTable;
import br.eng.rodrigogml.rfw.orm.dao.annotations.dao.RFWDAOAnnotation;
import br.eng.rodrigogml.rfw.orm.dao.annotations.dao.RFWDAOConverter;
import br.eng.rodrigogml.rfw.orm.dao.interfaces.DAOResolver;
import br.eng.rodrigogml.rfw.orm.dao.interfaces.RFWDAOConverterInterface;

/**
 * Description: Classe de DAO principal do Framework.<br>
 *
 * @author Rodrigo Leit�o
 * @since 10.0.0 (11 de jul de 2018)
 */
public final class RFWDAO<VO extends RFWVO> {

  /**
   * Configura o RFWDAO para o dialeto conforme a base de dados.
   */
  public enum SQLDialect {
    MySQL("`", false), DerbyDB("", true);

    /**
     * QuotationMark: caracter utilizado como 'aspas' em volta dos nomes de tabelas e colunas. MySQL: ', Derby: nenhum.
     */
    private final String qM;

    /**
     * Indica se ao criar o statement de insert n�o escrever ID atribuindo valor igual a null.<br>
     * No Derby, quando temos uma coluna de IDs gerada automaticamente ela n�o pode aparecer no statement.
     */
    private final boolean skipInsertIDColumn;

    private SQLDialect(String quotationMark, boolean skipInsertIDColumn) {
      this.qM = quotationMark;
      this.skipInsertIDColumn = skipInsertIDColumn;
    }

    /**
     * # quotationMark: caracter utilizado como 'aspas' em volta dos nomes de tabelas e colunas. MySQL: ', Derby: nenhum.
     *
     * @return the quotationMark: caracter utilizado como 'aspas' em volta dos nomes de tabelas e colunas
     */
    public String getQM() {
      return qM;
    }

    /**
     * # indica se ao criar o statement de insert n�o escrever ID atribuindo valor igual a null.<br>
     * No Derby, quando temos uma coluna de IDs gerada automaticamente ela n�o pode aparecer no statement.
     *
     * @return the indica se ao criar o statement de insert n�o escrever ID atribuindo valor igual a null
     */
    public boolean getSkipInsertIDColumn() {
      return skipInsertIDColumn;
    }

  }

  /**
   * Objeto utilizado para registrar pend�ncias de inser��o de objetos cruzados.<br>
   * Por exemplo, o Framework precisa inserir um objeto que tem uma associa��o com outro que ainda n�o foi inserido (ainda n�o tem um ID).<br>
   * Nestes caso a l�gica de persist�ncia cria um objeto desses registrando que o objeto foi persistido, mas que � necess�rio atualizar a associa��o quando o outro objeto estiver persistido tamb�m.<br>
   * Esta estrutura � utilizada principalmente em casos do {@link RelationshipTypes#INNER_ASSOCIATION}.
   */
  private static class RFWVOUpdatePending<RFWVOO> {

    /**
     * Caminho desde o VO base at� a o {@link #entityVO}.
     */
    private final String path;
    /**
     * EntityVO que foi persistido sem completar a FK esperando o objeto da FK ser persistido.
     */
    private final RFWVO entityVO;
    /**
     * Propriedade da refer�ncia que foi definida como NULL para que o objeto pudesse ser persistido.
     */
    private final String property;
    /**
     * VO que deve ganhar o ID at� o final da persist�ncia, e ser redefinido em {@link #property} do {@link #entityVO} para terminar a persist�ncia.
     */
    private final RFWVO fieldValueVO;

    public RFWVOUpdatePending(String path, RFWVO entityVO, String property, RFWVO fieldValueVO) {
      this.path = path;
      this.entityVO = entityVO;
      this.property = property;
      this.fieldValueVO = fieldValueVO;
    }

    /**
     * # entityVO que foi persistido sem completar a FK esperando o objeto da FK ser persistido.
     *
     * @return the entityVO que foi persistido sem completar a FK esperando o objeto da FK ser persistido
     */
    public RFWVO getEntityVO() {
      return entityVO;
    }

    /**
     * # propriedade da refer�ncia que foi definida como NULL para que o objeto pudesse ser persistido.
     *
     * @return the propriedade da refer�ncia que foi definida como NULL para que o objeto pudesse ser persistido
     */
    public String getProperty() {
      return property;
    }

    /**
     * # vO que deve ganhar o ID at� o final da persist�ncia, e ser redefinido em {@link #property} do {@link #entityVO} para terminar a persist�ncia.
     *
     * @return the vO que deve ganhar o ID at� o final da persist�ncia, e ser redefinido em {@link #property} do {@link #entityVO} para terminar a persist�ncia
     */
    public RFWVO getFieldValueVO() {
      return fieldValueVO;
    }

    /**
     * # caminho desde o VO base at� a o {@link #entityVO}.
     *
     * @return the caminho desde o VO base at� a o {@link #entityVO}
     */
    public String getPath() {
      return path;
    }
  }

  /**
   * Dialeto utilizado na cria��o dos SQLs.<br>
   * Valor Padr�o MySQL.
   */
  private final SQLDialect dialect;

  /**
   * DataSource para o qual o DAO foi criado.
   */
  private final DataSource ds;

  /**
   * Define o tipo da classe que estamos trabalhando nest RFWDAO. Permite definir a tabela, schema de trabalho, etc.
   */
  private final Class<VO> type;

  /**
   * Schema a ser utilizado no SQL. Se n�o for definido, ser� utilizado o schema que estiver definido na Entidade.
   */
  private final String schema;

  /**
   * Interface utilizada pela aplica��o apra resolver informa��es sobre a entidade.
   */
  private final DAOResolver resolver;

  /**
   * Cria um RFWDAO que for�a a utiliza��o de um determinado Schema, ao inv�s de utilizar o schema da sess�o do usu�rio. <br>
   * Este construtor permite passar um DataSource espec�fico. Podendo inclusive ser implementado manualmente para retornar conex�es com o banco de dados de forma Local.<br>
   * <br>
   * <b>Aten��o:</b> Note que algumas informa��es, com o schema a ser utilizado, podem ser definidas de v�rias maneiras: na Entidade, no {@link RFWDAO}, pelo {@link DAOResolver}.<Br>
   * Nesses casos, o {@link RFWDAO} usar� a informa��o conforme disponibilidade de acordo com a seguinte hierarquia:
   * <li>solicita {@link DAOResolver}, se este n�o existir ou retornar nulo;
   * <li>solicita informa��o do RFWDAO, se este n�o tiver a informa��o ou estiver nula;
   * <li>Verificamos a defini��o da annotation {@link br.eng.rodrigogml.rfw.base.dao.annotations.dao.RFWDAO} da entidade. Se esta n�o tiver, resultar� em Exception
   *
   * @param type Objeto que ser� manipulado no banco de dados.
   * @param schema Schema a ser utilizado.
   * @param ds DataSource respons�vel por entregar conex�es sob demanda.
   * @param dialect Defina o dialeto do banco de dados. Embora o comando SQL seja �nico, cada banco possui diferen�as que interferem no funcionamento da classe.
   * @param resolver interface para deixar o {@link RFWDAO} mais din�mico. Entre as informa��es est� o pr�prio Schema e tabela de cada entidade. Caso nenhum {@link DAOResolver} seja passado, ou este retorne nulo nas suas informa��es, as informa��es passardas no RFWDAO passam a ser utilizadas.
   * @throws RFWException
   */
  public RFWDAO(Class<VO> type, String schema, DataSource ds, SQLDialect dialect, DAOResolver resolver) throws RFWException {
    this.type = type;
    this.schema = schema;
    this.ds = ds;
    this.dialect = dialect;
    this.resolver = resolver;
  }

  /**
   * Exclui uma entidade do banco de dados.
   *
   * @param ids ID do objeto a ser exclu�do
   * @throws RFWException
   *           <li>RFW_ERR_000006 - Critical em caso de falha de Constraint.
   */
  public void delete(Long... ids) throws RFWException {
    // A exclus�o � focada apenas na exclus�o do objeto principal, uma vez que a resti��o quando o objeto est� em uso ou de objetos de composi��o deve estar implementada adequadamente no banco de dados.
    final DAOMap map = createDAOMap(this.type, null);

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createDeleteStatement(conn, map, "", dialect, ids)) {
      stmt.executeUpdate();
    } catch (java.sql.SQLIntegrityConstraintViolationException e) {
      // Se a dele��o falha por motivos de constraints � poss�vel que o objeto n�o possa ser apagado. Neste caso h� m�todos do CRUD que ao inv�s de excluir o objeto o desativam, por isso enviamos uma exception diferente
      throw new RFWCriticalException("RFW_ERR_000006", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
  }

  public void massUpdate(Map<String, Object> setValues, RFWMO mo) throws RFWException {
    String[] updateAttributes = setValues.keySet().toArray(new String[0]);
    updateAttributes = RUArray.concatAll(updateAttributes, mo.getAttributes().toArray(new String[0]));
    final DAOMap map = createDAOMap(this.type, updateAttributes);

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createMassUpdateStatement(conn, map, setValues, mo, this.type, dialect)) {
      stmt.executeUpdate();
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao atualizar em massa os elementos no banco de dados!", e);
    }
  }

  /**
   * M�todo utilizado para persistir um objeto. Objetos com ID ser�o atualizados, objetos sem ID � considerado para inser��o.
   *
   * @param vo Objeto a ser persistido.
   * @return Objeto com todos os IDs criados.
   * @throws RFWException
   */
  public VO persist(VO vo) throws RFWException {
    return persist(vo, false);
  }

  /**
   * M�todo utilizado para persistir um objeto. Objetos com ID ser�o atualizados, objetos sem ID � considerado para inser��o.
   *
   * @param vo Objeto a ser persistido.
   * @param ignoreFullLoaded Permite ignorar a verifica��o se um objeto que ser� persistido n�o foi recuperado completamente para atualiza��o. Essa op��o s� deve ser utilizada em casos muito espec�ficos e preferencialmente quando for poss�vel aplicar outra solu��o, n�o utilizar essa, utilizar o {@link #findForUpdate(Long, String[])} sempre que poss�vel.
   * @return Objeto com todos os IDs criados.
   * @throws RFWException
   */
  @SuppressWarnings("deprecation")
  public VO persist(VO vo, boolean ignoreFullLoaded) throws RFWException {
    boolean isNew = vo.getId() == null || vo.isInsertWithID();

    // Para garantir que n�o vamos estragar objetos pq o desenvolvedor est� enviando objetos incompletos para persist�ncia (fazendo com que o RFWDAO exclusi composi��es e associa��es, por exemplo) obrigados que o objeto sendo persistido tenha sido obtido atrav�s "findForUpdate()" ou similares.
    // Esta � uma medida de seguran�a para evitar que dados sejam estragador por alguma parte do sistema que n�o tenha sido atualizada depois que a estrutura de algum objeto tenha sido alterada.
    // N�O REMOVER, NEM NUNCA DEFINIR MANUALMENTE O VALOR DE ISFULLLOADED FORA DO RFWDAO!!!
    if (!ignoreFullLoaded && !isNew && !vo.isFullLoaded()) throw new RFWCriticalException("O RFWDAO s� aceita persistir objetos que foram completamente carregados para edi��o!");

    final String[] updateAttributes = RUReflex.getRFWVOUpdateAttributes(vo.getClass());
    final DAOMap map = createDAOMap(this.type, updateAttributes);

    final HashMap<String, VO> persistedCache = new HashMap<>(); // Cache para armazenas os objetos que j� foram persistidos. Evitando assim cair em loop ou m�ltiplas atualiza��es no banco de dados.

    VO originalVO = null;
    if (!isNew) originalVO = findForUpdate(vo.getId(), null);

    HashMap<RFWVO, List<RFWVOUpdatePending<RFWVO>>> updatePendings = new HashMap<RFWVO, List<RFWVOUpdatePending<RFWVO>>>();
    persist(ds, map, isNew, vo, originalVO, "", persistedCache, null, 0, updatePendings, dialect);

    if (updatePendings.size() > 0) {
      for (List<RFWVOUpdatePending<RFWVO>> pendList : updatePendings.values()) {
        for (RFWVOUpdatePending<RFWVO> pendBean : pendList) {
          if (pendBean.getFieldValueVO().getId() == null) {
            throw new RFWCriticalException("Falha ao completar os objetos pendentes! Mesmo deixando para atualizar a refer�ncia depois do objeto persistido, alguns objetos continuaram sem IDs para validar as FKs.");
          }
          updateInternalFK(ds, map, pendBean.getPath(), pendBean.getProperty(), pendBean.getEntityVO().getId(), pendBean.getFieldValueVO().getId(), dialect);
        }
      }
    }
    vo.setInsertWithID(false); // Garante que o objeto n�o vai retornar com a flag em true. Um objeto que tenha ID mas que tenha essa flag em true � considerado pelo sistema como um objeto que n�o est� no banco de dados.
    return vo;
  }

  /**
   * Este m�todo permite que apenas os atributos passados sejam atualizados no banco de dados.<br>
   * O m�todo produt o objeto no banco de acordo com sua classe e 'id' definidos, e copia os valores dos atributos definidos em 'attributes' do VO recebido para o objeto obtido do banco de dados, garantindo assim que apenas os valores selecionados ser�o atualizados.<Br>
   * <bR>
   * <B>Observa��o de Usos:</b> Propriedades aninhadas podem ser utilizadas, mas apenas em casos bem espec�ficos:
   * <ul>
   * <li>Cuidado ao definir propriedades aninhadas, este m�todo n�o inicaliza os objetos se eles vierem nulos do banco de dados, pois nesses casos � necess�rio enviar o objeto completo e validado.
   * <li>Apenas o objeto principal ser� persistido no banco de dados, assim, definir propriedades aninhadas em objetos com relacionamento de associa��o ou parent_association, por exemplo, n�o far� com que o valor seja atualizado.
   * </ul>
   *
   * @param vo Objeto com o id de identifica��o e os valores que devem ser atualizados.
   * @param attributes Array com os atributos que devem ser copiados para o objeto original antes de ser persistido.
   * @return
   * @throws RFWException
   * @Deprecated Este m�todo foi criado para retrocompatibilidade com o BIS2 e deve ser apagado em breve. A alternativa desse m�todo � utilizar o {@link #massUpdate(Map, RFWMO)} com um filtro pelo ID.<br>
   *             Ou podemos criar um novo m�todo chamado algo como 'simpleUpdate' ou 'uniqueUpdate', que ao inv�s do MO receba o ID direto do objeto (e internamente direciona o para o massUpdate.
   */
  @Deprecated
  public VO persist(VO vo, String[] attributes) throws RFWException {
    PreProcess.requiredNonNull(vo);
    PreProcess.requiredNonNull(vo.getId());
    PreProcess.requiredNonEmptyCritical(attributes);

    VO dbVO = findForUpdate(vo.getId(), attributes);

    for (String att : attributes) {
      Object value = RUReflex.getPropertyValue(vo, att);
      RUReflex.setPropertyValue(dbVO, att, value, false);
    }

    return persist(dbVO);
  }

  @SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
  private void persist(DataSource ds, DAOMap daoMap, boolean isNew, VO entityVO, VO entityVOOrig, String path, HashMap<String, VO> persistedCache, String sortColumn, int sortIndex, HashMap<RFWVO, List<RFWVOUpdatePending<RFWVO>>> updatePendings, SQLDialect dialect) throws RFWException {
    if (isNew && entityVO.getId() != null && !entityVO.isInsertWithID()) {
      throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. A entidade j� veio com o ID definido para inser��o.", new String[] { entityVO.getClass().getCanonicalName() });
    }
    if (isNew || !persistedCache.containsKey(entityVO.getClass().getCanonicalName() + "." + entityVO.getId())) {
      int parentCount = 0;
      boolean needParent = false; // Flag para indicar se encontramos algum PARENT_ASSOCIATION. Se o objeto tiver algum objeto com relacionamento do tipo Parent, torna-se obrigat�rio ter um parent deifnido
      // ===> TRATAMENTO DO RELACIONAMENTO ANTES DE INSERIR O OBJETO <===
      for (Field field : entityVO.getClass().getDeclaredFields()) {
        final RFWMetaRelationshipField ann = field.getAnnotation(RFWMetaRelationshipField.class);
        if (ann != null) {
          // Verificamos o tipo de relacionamento para validar e saber como proceder.
          switch (ann.relationship()) {
            case WEAK_ASSOCIATION:
              // Nada para fazer, esse tipo de associa��o � como se n�o existisse para o RFWDAO.
              break;
            case PARENT_ASSOCIATION: {
              needParent = true;
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());

              // DELETE: Atributos de parentAssociation n�o h� nada para fazer em rela��o a exclus�o, j� que quem nunca exclu�mos o pai, pelo contr�rio, � ele quem nos exclu�.
              // PERSISTENCE: nada a fazer com o objeto pai al�m da valida��o abaixo
              // VALIDA: Cada objeto de composi��o s� pode ter 1 pai (Um objeto pode ser reutilizado como filho de outro objeto, mas ele s� pode ter um objeto pai definido).
              if (fieldValue != null) parentCount++;
              if (parentCount > 1) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Encontramos mais de um relacionamento marcado como \"Parent Association\". Cada objeto de composi��o s� pode ter 1 pai.", new String[] { entityVO.getClass().getCanonicalName() });

              // VALIDA: Se o objeto pai for obrigat�rio, se j� tem ID
              if (fieldValue == null) {
                // Parent Association se for obrigat�rio
                if (ann.required()) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associa��o de pai com objeto nulo ou sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
              } else {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  RFWVO fieldValueVO = (RFWVO) fieldValue;
                  // Nos casos de Parent_Association, n�o precisamos fazer nada pq o pai j� deve ter sido inserido e ter o seu ID pronto antes do filho ser chamado para inser��o. S� validamos isso
                  if (fieldValueVO.getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associa��o de pai com objeto nulo ou sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                } else {
                  // Parent Association n�o pode ter nada que n�o seja um RFWVO
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
            case INNER_ASSOCIATION: {
              // VALIDA��O: No caso de INNER_ASSOCIATION, ou o atributo column ou columnMapped devem estar preenchidos
              if ("".equals(getMetaRelationColumnMapped(field, ann)) && "".equals(getMetaRelationColumn(field, ann))) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' est� marcado como relacionamento 'Inner Association', este tipo de relacionamento deve ter os atirbutos 'column' ou 'columnMapped' preenchidos.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });

              // DELETE: quando o ID est� neste objeto, sendo ele exclu�do ou a associa��o desfeita o ID tudo se resolve ao excluir ou atualizar este objeto. No caso de estar na tabela da contraparte, vamos atualizar ela depois que exclu�rmos esse objeto.
              // PERSIST�NCIA: na persist�ncia, por ser um objeto que est� sendo persistido agora, pode ser que j� tenhamos o ID, pode ser que n�o. Se j� tiver o ID, deixa seguir, se n�o tiver, vamos limpar a associa��o para que se possa inserir o objeto sem a associa��o. e colocar o objeto na lista de pend�ncias para atualizar a associa��o depois que tudo tiver sido persistido.
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  VO fieldValueVO = (VO) fieldValue;
                  if (fieldValueVO.getId() == null) {
                    RUReflex.setPropertyValue(entityVO, field.getName(), null, false);
                    List<RFWVOUpdatePending<RFWVO>> pendList = updatePendings.get(fieldValueVO);
                    if (pendList == null) {
                      pendList = new LinkedList<RFWDAO.RFWVOUpdatePending<RFWVO>>();
                      updatePendings.put(fieldValueVO, pendList);
                    }
                    pendList.add(new RFWVOUpdatePending(path, entityVO, field.getName(), fieldValueVO));
                  }
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  List list = (List) fieldValue;
                  for (Object item : list) {
                    VO fieldValueVO = (VO) item;
                    if (fieldValueVO.getId() == null) {
                      RUReflex.setPropertyValue(entityVO, field.getName(), null, false);
                      List<RFWVOUpdatePending<RFWVO>> pendList = updatePendings.get(fieldValueVO);
                      if (pendList == null) {
                        pendList = new LinkedList<RFWDAO.RFWVOUpdatePending<RFWVO>>();
                        updatePendings.put(fieldValueVO, pendList);
                      }
                      pendList.add(new RFWVOUpdatePending(path, entityVO, field.getName(), fieldValueVO));
                    }
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  Map map = (Map) fieldValue;
                  for (Object item : map.values()) {
                    VO fieldValueVO = (VO) item;
                    if (fieldValueVO.getId() == null) {
                      RUReflex.setPropertyValue(entityVO, field.getName(), null, false);
                      List<RFWVOUpdatePending<RFWVO>> pendList = updatePendings.get(fieldValueVO);
                      if (pendList == null) {
                        pendList = new LinkedList<RFWDAO.RFWVOUpdatePending<RFWVO>>();
                        updatePendings.put(fieldValueVO, pendList);
                      }
                      pendList.add(new RFWVOUpdatePending(path, entityVO, field.getName(), fieldValueVO));
                    }
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case COMPOSITION: {
              // PERSIST�NCIA: Em caso de composi��o, n�o fazemos nada aqui no pr�-processamento, pois os objetos compostos ser�o persistidos depois do objeto pai.
              // DELETE: Relacionamento de Composi��o, precisamos verificar se ele existia antes e deixou de existir, ou em caso de 1:N verifica quais objetos deixaram de existir.
              // ATEN��O: N�o aceita as cole��es nulas pq, por defini��o, objeto nulo indica que n�o foi recuperado, a aus�ncia de objetos relacionados deve ser sempre simbolizada por uma lista vazia.
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    RFWVO fieldValueVO = (RFWVO) fieldValue;
                    RFWVO fieldValueVOOrig = (RFWVO) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if (fieldValueVOOrig != null && (fieldValueVO == null || fieldValueVO.getId() == null)) {
                      // Se o objeto no banco existir e o objeto atual for diferente ou n�o tiver ID, temos de excluir o objeto atual pq o objeto mudou.
                      delete(ds, daoMap, fieldValueVOOrig, RUReflex.addPath(path, field.getName()), dialect);
                    }
                  }
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    List list = (List) fieldValue;
                    List listOrig = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if ((list == null || list.size() == 0) && (listOrig != null && listOrig.size() > 0)) {
                      // Se n�o temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
                      for (Object item : listOrig) {
                        delete(ds, daoMap, (VO) item, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    } else if ((listOrig != null && listOrig.size() >= 0) && (list != null && list.size() >= 0)) {
                      // Se temos as duas listas, temos que comprar as duas e descobrir os objetos que sumiram, assim iteramos uma dentro da outra para ver os objetos que sumiram comparando seus IDs
                      for (Object itemOrig : listOrig) {
                        VO itemOrigVO = (VO) itemOrig;
                        boolean found = false;
                        for (Object item : list) {
                          VO itemVO = (VO) item;
                          if (itemOrigVO.getId().equals(itemVO.getId())) {
                            found = true;
                            break;
                          }
                        }
                        if (!found) delete(ds, daoMap, itemOrigVO, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    } else if ((listOrig == null || listOrig.size() == 0) && (list == null || list.size() == 0)) {
                      // Se n�o temos lista agora, e j� n�o tinhamos, nada a fazer. O IF s� previne cair no else e lan�ar a Exception de "preven��o de falha de l�gica".
                    } else {
                      throw new RFWCriticalException("Falha ao detectar a condi��o de compara��o entre listas do novo objeto e do objeto anterior! Atributo '${0}' da Classe '${1}'.", new String[] { field.getName(), entityVO.getClass().getCanonicalName() });
                    }
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    Map hash = (Map) fieldValue;
                    Map hashOrig = (Map) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if (hash.size() == 0 && hashOrig.size() > 0) {
                      // Se n�o temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
                      for (Object itemOrig : hashOrig.values()) {
                        delete(ds, daoMap, (VO) itemOrig, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    } else if (hashOrig.size() > 0 && hash.size() > 0) {
                      // Se temos as duas listas, temos que comprar as duas e descobrir os objetos que sumiram, assim iteramos uma dentro da outra para ver os objetos que sumiram comparando seus IDs
                      for (Object itemOrig : hashOrig.values()) {
                        VO itemOrigVO = (VO) itemOrig;
                        boolean found = false;
                        for (Object item : hash.values()) {
                          VO itemVO = (VO) item;
                          if (itemOrigVO.getId().equals(itemVO.getId())) {
                            found = true;
                            break;
                          }
                        }
                        if (!found) delete(ds, daoMap, itemOrigVO, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    }
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              } else {
                // Se n�o existe no objeto atual, verificamos se existe no objeto original
                if (entityVOOrig != null) {
                  final Object fieldValueOrig = RUReflex.getPropertyValue(entityVOOrig, field.getName());
                  if (fieldValueOrig != null) {
                    if (RFWVO.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Se o objeto no banco existir e o objeto atual n�o, temos de excluir o objeto atual pq a composi��o mudou.
                      delete(ds, daoMap, (VO) fieldValueOrig, RUReflex.addPath(path, field.getName()), dialect);
                    } else if (List.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Se n�o temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
                      for (Object item : (List) fieldValueOrig) {
                        delete(ds, daoMap, (VO) item, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    } else if (Map.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Se n�o temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
                      for (Object itemOrig : ((Map) fieldValueOrig).values()) {
                        delete(ds, daoMap, (VO) itemOrig, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    }
                  }
                }
              }
            }
              break;
            case COMPOSITION_TREE: {
              // PERSIST�NCIA: Em caso de composi��o, n�o fazemos nada aqui no pr�-processamento, pois os objetos compostos ser�o persistidos depois do objeto pai.
              // DELETE: Relacionamento de Composi��o, precisamos verificar se ele existia antes e deixou de existir. Se ele deixou de existir, precisamos excluir todas sua hierarquia.
              // ATEN��O: N�o aceita as cole��es nulas pq, por defini��o, objeto nulo indica que n�o foi recuperado, a aus�ncia de objetos relacionados deve ser sempre simbolizada por uma lista vazia.
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  throw new RFWValidationException("Encontrado a defini��o 'COMPOSITION_TREE' em um relacionamento 1:1. Essa defini��o s� pode ser utilizado em cole��es para indicar os 'filhos' do relacionamento hierarquico. Classe: ${0} / Field: ${1} / FieldClass: ${2}.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    List list = (List) fieldValue;
                    List listOrig = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if ((list == null || list.size() == 0) && (listOrig != null && listOrig.size() > 0)) {
                      // Se n�o temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
                      for (Object item : listOrig) {
                        String destPath = RUReflex.addPath(path, field.getName());
                        // Se n�o tivermos o caminho temos de completar dianimicamente no DAOMap
                        if (daoMap.getMapTableByPath(destPath) == null) daoMap.createMapTableForCompositionTree(path, destPath, "id", getMetaRelationColumnMapped(field, ann));
                        delete(ds, daoMap, (VO) item, destPath, dialect);
                      }
                    } else if ((listOrig != null && listOrig.size() >= 0) && (list != null && list.size() >= 0)) {
                      // Se temos as duas listas, temos que comprar as duas e descobrir os objetos que sumiram, assim iteramos uma dentro da outra para ver os objetos que sumiram comparando seus IDs
                      for (Object itemOrig : listOrig) {
                        VO itemOrigVO = (VO) itemOrig;
                        boolean found = false;
                        for (Object item : list) {
                          VO itemVO = (VO) item;
                          if (itemOrigVO.getId().equals(itemVO.getId())) {
                            found = true;
                            break;
                          }
                        }
                        if (!found) delete(ds, daoMap, itemOrigVO, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    } else if ((listOrig == null || listOrig.size() == 0) && (list == null || list.size() == 0)) {
                      // Se n�o temos lista agora, e j� n�o tinhamos, nada a fazer. O IF s� previne cair no else e lan�ar a Exception de "preven��o de falha de l�gica".
                    } else {
                      throw new RFWCriticalException("Falha ao detectar a condi��o de compara��o entre listas do novo objeto e do objeto anterior! Atributo '${0}' da Classe '${1}'.", new String[] { field.getName(), entityVO.getClass().getCanonicalName() });
                    }
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    Map hash = (Map) fieldValue;
                    Map hashOrig = (Map) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if (hash.size() == 0 && hashOrig.size() > 0) {
                      // Se n�o temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
                      for (Object itemOrig : hashOrig.values()) {
                        delete(ds, daoMap, (VO) itemOrig, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    } else if (hashOrig.size() > 0 && hash.size() > 0) {
                      // Se temos as duas listas, temos que comprar as duas e descobrir os objetos que sumiram, assim iteramos uma dentro da outra para ver os objetos que sumiram comparando seus IDs
                      for (Object itemOrig : hashOrig.values()) {
                        VO itemOrigVO = (VO) itemOrig;
                        boolean found = false;
                        for (Object item : hash.values()) {
                          VO itemVO = (VO) item;
                          if (itemOrigVO.getId().equals(itemVO.getId())) {
                            found = true;
                            break;
                          }
                        }
                        if (!found) delete(ds, daoMap, itemOrigVO, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    }
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case ASSOCIATION: {
              // VALIDA��O: No caso de associa��o, ou o atributo column ou columnMapped devem estar preenchidos
              if ("".equals(getMetaRelationColumnMapped(field, ann)) && "".equals(getMetaRelationColumn(field, ann))) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' est� marcado como relacionamento 'Association', este tipo de relacionamento deve ter os atirbutos 'column' ou 'columnMapped' preenchidos.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });

              // DELETE: nos casos de associa��o, quando o ID est� na nossa tabela, ele ser� definido como null ao atualizar o objeto e n�o devemos apagar a contra-parte. No caso do ID estar na tabela da contra-parte, vamos defini-lo como nulo depois do persistir o objeto atualizado
              // PERSIST�NCIA: Nos casos de associa��o � esperado que o objeto associado j� tenha um ID definido, j� que � um objeto a parte
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  VO fieldValueVO = (VO) fieldValue;
                  if (fieldValueVO.getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associa��o sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  List list = (List) fieldValue;
                  for (Object item : list) {
                    if (((VO) item).getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associa��o sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  Map map = (Map) fieldValue;
                  for (Object item : map.values()) {
                    if (((VO) item).getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associa��o sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case MANY_TO_MANY:
              // DELETE: os relacionamentos N:N ser�o exclu�dos depois da atualiza��o do objeto
              // PERSIST�NCIA: Nos casos de ManyToMany a coluna de FK n�o est� na tabela do objeto (e sim na tabela de joinAlias). Por isso tudo o que temos que fazer � validar se todos os objetos tem um ID para a posterior inser��o.
              // PERSIST�NCIA: Note que ManyToMany deve sempre estar dentro de algum tipo de cole��o/lista/hash/etc por ser m�ltiplos objetos.
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue == null) {
                // Por ser esperado sempre uma Lista nas associa��es ManyToMany, um objeto recebido nulo � um erro, j� que nulo indica que n�o foi carregado enquanto que uma cole��o vazia indica a aus�ncia de associa��es.
                throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. No atributo '${1}' recebemos uma cole��o vazia. A aus�ncia de relacionamento deve sempre ser indicada por uma cole��o vazia, o atributo nulo � indicativo de que ele n�o foi carredo do banco de dados.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
              } else {
                if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  List list = (List) fieldValue;
                  for (Object item : list) {
                    if (((VO) item).getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associa��o sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  Map hash = (Map) fieldValue;
                  for (Object item : hash.values()) {
                    if (((VO) item).getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associa��o sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
              break;
          }
        }
        final RFWMetaCollectionField colAnn = field.getAnnotation(RFWMetaCollectionField.class);
        if (colAnn != null) {
          // No caso de lista, temos de excluir todos os objetos anteriores do banco para inserir os novos depois. N�o temos como comparar pq n�o utilizamos IDs nesses objetos. Assim, se o objeto original existir exclu�mos todos os itens associados anteriormente de uma �nica vez.
          if (entityVOOrig != null) {
            deleteCollection(ds, daoMap, entityVOOrig, "@" + RUReflex.addPath(path, field.getName()), dialect);
          }
        }
      }

      if (needParent && parentCount == 0) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. H� relacionamentos do tipo 'PARENT_ASSOCIATION', o que indica que o objeto � dependente de outro, mas nenhum relacionamento desse tipo foi definido!", new String[] { entityVO.getClass().getCanonicalName() });

      // ===> INSERIMOS O OBJETO NO BANCO <===
      try (Connection conn = ds.getConnection()) {
        if (isNew) {
          try (PreparedStatement stmt = DAOMap.createInsertStatement(conn, daoMap, path, entityVO, sortColumn, sortIndex, dialect)) {
            stmt.executeUpdate();
            try (ResultSet rs = stmt.getGeneratedKeys()) {
              Long id = null;
              if (rs.next()) {
                id = rs.getLong(1);
              } else {
                throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O ID n�o foi retornado pelo banco de dados. Verifique se a coluna 'id' gera as chaves automaticamente.", new String[] { entityVO.getClass().getCanonicalName() });
              }
              entityVO.setId(id);
            }
          }
        } else {
          try (PreparedStatement stmt = DAOMap.createUpdateStatement(conn, daoMap, path, entityVO, sortColumn, sortIndex, dialect)) {
            stmt.executeUpdate();
          }
        }

        persistedCache.put(entityVO.getClass().getCanonicalName() + "." + entityVO.getId(), entityVO);

      } catch (Throwable e) {
        throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
      }

      // ===> PROCESSAMENTO DOS RELACIONAMENTOS P�S INSER��O DO OBJETO
      for (Field field : entityVO.getClass().getDeclaredFields()) {
        final RFWMetaRelationshipField ann = field.getAnnotation(RFWMetaRelationshipField.class);
        if (ann != null) {
          // Verificamos o tipo de relacionamento para validar e saber como proceder.
          switch (ann.relationship()) {
            case WEAK_ASSOCIATION:
              // Nada para fazer, esse tipo de associa��o � como se n�o existisse para o RFWDAO.
              break;
            case ASSOCIATION:
              // No caso de associa��o e a FK estar na tabela do outro objeto, temos atualizar a coluna do outro objeto. (Se estiver na tabela do objeto sendo editado o valor j� foi definido)
              if (!"".equals(getMetaRelationColumnMapped(field, ann))) {
                // Verificamos se houve altera��o entre a associa��o atual e a associa��o existente no banco de dados para saber se precisamos atualizar a tabels
                final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
                if (fieldValue != null) { // Atualmente temos um relacionamento
                  if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                    RFWVO fieldValueVO = (RFWVO) fieldValue;
                    RFWVO fieldValueVOOrig = null;
                    if (entityVOOrig != null) fieldValueVOOrig = (RFWVO) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if (fieldValueVOOrig != null && !fieldValueVO.getId().equals(fieldValueVOOrig.getId())) {
                      // Se tamb�m temos um relacionamento no VO original e eles tem IDs diferentes, precisamos remover a associa��o do objeto anterior antes de incluir a nova associa��o (se tem o mesmo ID n�o precisamos fazer nada pois j� est�o certos)
                      updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueVOOrig.getId(), null, dialect); // Exclui a associa��o do objeto anterior
                    }
                    // Agora que j� removemos as associa��es do objeto que n�o est�o mais em uso, vamos atualizar as novas associa��es.
                    updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueVO.getId(), entityVO.getId(), dialect); // Inclui a associa��o do novo Objeto
                  } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                    Map fieldValueMap = (Map) fieldValue;
                    Map fieldValueMapOrig = null;
                    if (entityVOOrig != null) fieldValueMapOrig = (Map) RUReflex.getPropertyValue(entityVOOrig, field.getName());

                    if (fieldValueMapOrig != null && fieldValueMapOrig.size() > 0) {
                      // Se tamb�m temos um relacionamento no VO original, iteramos seus objetos para compara��o...
                      for (Object key : fieldValueMapOrig.keySet()) {
                        RFWVO fieldValueVOOrig = (RFWVO) fieldValueMapOrig.get(key);
                        RFWVO fieldValueVO = (RFWVO) fieldValueMap.get(key);
                        if (fieldValueVO == null || !fieldValueVO.getId().equals(fieldValueVOOrig.getId())) {
                          // ..., temos o objeto para as mesma chavez, mas eles tem IDs diferentes, precisamos remover a associa��o antiga (a nova associa��o � feita depois) (se tem o mesmo ID n�o precisamos fazer nada pois j� est�o certos)
                          updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueVOOrig.getId(), null, dialect); // Exclui a associa��o do objeto anterior na tabela
                        }
                      }
                    }
                    // Tendo ou n�o removido associa��es dos objetos que n�o est�o mais associados, atualizamos os novos objetos associados
                    for (Object obj : fieldValueMap.values()) {
                      RFWVO fieldValueVO = (RFWVO) obj;
                      updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueVO.getId(), entityVO.getId(), dialect); // Atualiza a associa��o na tabela do objeto associado.
                    }
                  } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                    List list = (List) fieldValue;
                    List listOriginal = null;
                    if (entityVOOrig != null) listOriginal = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());

                    if (listOriginal != null && listOriginal.size() > 0) {
                      // Se tamb�m temos um relacionamento no VO original, iteramos seus objetos para compara��o...
                      for (Object itemOriginal : listOriginal) {
                        RFWVO itemVOOrig = (RFWVO) itemOriginal;
                        RFWVO itemVO = null;
                        if (list != null) {
                          // Se temos uma lista do objeto atual, vamos tentar encontrar o objeto para atualiza��o
                          for (Object item : list) {
                            if (itemVOOrig.getId().equals(((VO) item).getId())) { // ItemOriginal sempre tem um ID pois veio do banco de dados.
                              itemVO = (VO) item;
                              break;
                            }
                          }
                        }

                        if (itemVO == null || !itemVOOrig.getId().equals(itemVO.getId())) {
                          // ..., temos o objeto em ambas a lista, mas eles tem IDs diferentes, precisamos remover a associa��o antiga (a nova associa��o � feita depois) (se tem o mesmo ID n�o precisamos fazer nada pois j� est�o certos)
                          updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), itemVOOrig.getId(), null, dialect); // Exclui a associa��o do objeto anterior na tabela
                        }
                      }
                    }
                    // Tendo ou n�o removido associa��es dos objetos que n�o est�o mais associados, atualizamos os novos objetos associados
                    for (Object item : list) {
                      RFWVO itemVO = (RFWVO) item;
                      updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), itemVO.getId(), entityVO.getId(), dialect); // Atualiza a associa��o na tabela do objeto associado.
                    }
                  } else {
                    throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                  }
                } else {
                  // Se n�o temos uma associa��o no objeto atual, temos que remover da antiga caso exista
                  Object fieldValueOrig = null;
                  if (entityVOOrig != null) fieldValueOrig = RUReflex.getPropertyValue(entityVOOrig, field.getName());
                  if (fieldValueOrig != null) {
                    if (RFWVO.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      RFWVO fieldValueOrigVO = (RFWVO) fieldValueOrig;
                      updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueOrigVO.getId(), null, dialect); // Exclui a associa��o na tabela do objeto anterior
                    } else if (List.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Caso no objeto original tenha uma list lan�amos erro. Pois o objeto sendo persistido n�o deve ter as collections nulas e sim vazias para indicar a aus�ncia de associa��es. Uma collection nula provavelmente indica que o objeto n�o foi bem inicializado, ou mal recuperado do banco em caso de atualiza��o.
                      throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. No atributo '${1}' recebemos uma cole��o vazia. A aus�ncia de relacionamento deve sempre ser indicada por uma cole��o vazia, o atributo nulo � indicativo de que ele n�o foi carredo do banco de dados.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                    } else if (Map.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Caso no objeto original tenha uma hash lan�amos erro. Pois o objeto sendo persistido n�o deve ter as collections nulas e sim vazias para indicar a aus�ncia de associa��es. Uma collection nula provavelmente indica que o objeto n�o foi bem inicializado, ou mal recuperado do banco em caso de atualiza��o.
                      throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. No atributo '${1}' recebemos uma cole��o vazia. A aus�ncia de relacionamento deve sempre ser indicada por uma cole��o vazia, o atributo nulo � indicativo de que ele n�o foi carredo do banco de dados.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                    }
                  }
                }
              }
              break;
            case COMPOSITION: {
              // PERSIST�NCIA: Em caso de composi��o, temos agora que persistir todos os objetos filhos
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  VO fieldValueVOOrig = null;
                  if (entityVOOrig != null) fieldValueVOOrig = (VO) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                  // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composi��o n�o podem ter ID definido antes do pr�prio pai, provavelmente isso � um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composi��o novo (em insert).
                  persist(ds, daoMap, (isNew || ((VO) fieldValue).getId() == null), (VO) fieldValue, fieldValueVOOrig, RUReflex.addPath(path, field.getName()), persistedCache, null, 0, updatePendings, dialect);
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  List list = (List) fieldValue;
                  List listOriginal = null;
                  if (entityVOOrig != null) listOriginal = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());

                  // Se � uma lista, verificamos se tem o atributo "sortColumn" definido na Annotation. Nestes casos temos de criar esse atributo para ser salvo junto
                  String sColumn = null;
                  if (!"".equals(ann.sortColumn())) sColumn = ann.sortColumn();

                  int countIndex = 0; // Contador de indice. Usado para saber o �ndice do item na lista. Utilizado quando o sortColumn � definido para garantir a ordem da lista.
                  for (Object item : list) {
                    VO itemVO = (VO) item;
                    VO itemVOOrig = null;
                    if (listOriginal != null) {
                      // Se temos uma lista do objeto original, vamos tentar encontrar o objeto para passar como objeto original para compara��o
                      for (Object itemOriginal : listOriginal) {
                        if (((VO) itemOriginal).getId().equals(itemVO.getId())) { // ItemOriginal sempre tem um ID pois veio do banco de dados.
                          itemVOOrig = (VO) itemOriginal;
                          break;
                        }
                      }
                    }

                    // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composi��o n�o podem ter ID definido antes do pr�prio pai, provavelmente isso � um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composi��o novo (em insert).
                    persist(ds, daoMap, (isNew || itemVO.getId() == null), itemVO, itemVOOrig, RUReflex.addPath(path, field.getName()), persistedCache, sColumn, countIndex, updatePendings, dialect);

                    countIndex++;
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  Map hash = (Map) fieldValue;
                  Map hashOriginal = null;
                  if (entityVOOrig != null) hashOriginal = (Map) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                  for (Object key : hash.keySet()) {
                    VO itemVO = (VO) hash.get(key);
                    VO itemVOOrig = null;
                    if (hashOriginal != null) itemVOOrig = (VO) hashOriginal.get(key);
                    // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composi��o n�o podem ter ID definido antes do pr�prio pai, provavelmente isso � um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composi��o novo (em insert).
                    persist(ds, daoMap, (isNew || itemVO.getId() == null), itemVO, itemVOOrig, RUReflex.addPath(path, field.getName()), persistedCache, null, 0, updatePendings, dialect);
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case COMPOSITION_TREE: {
              // PERSIST�NCIA: Em caso de composi��o de �rvore, temos agora que persistir todos os objetos filhos
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  throw new RFWValidationException("Encontrado a defini��o 'COMPOSITION_TREE' em um relacionamento 1:1. Essa defini��o s� pode ser utilizado em cole��es para indicar os 'filhos' do relacionamento hierarquico. Classe: ${0} / Field: ${1} / FieldClass: ${2}.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  List list = (List) fieldValue;
                  List listOriginal = null;
                  if (entityVOOrig != null) listOriginal = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());

                  // Se � uma lista, verificamos se tem o atributo "sortColumn" definido na Annotation. Nestes casos temos de criar esse atributo para ser salvo junto
                  String sColumn = null;
                  if (!"".equals(ann.sortColumn())) sColumn = ann.sortColumn();

                  int countIndex = 0; // Contador de indice. Usado para saber o �ndice do item na lista. Utilizado quando o sortColumn � definido para garantir a ordem da lista.
                  for (Object item : list) {
                    VO itemVO = (VO) item;
                    VO itemVOOrig = null;
                    if (listOriginal != null) {
                      // Se temos uma lista do objeto original, vamos tentar encontrar o objeto para passar como objeto original para compara��o
                      for (Object itemOriginal : listOriginal) {
                        if (((VO) itemOriginal).getId().equals(itemVO.getId())) { // ItemOriginal sempre tem um ID pois veio do banco de dados.
                          itemVOOrig = (VO) itemOriginal;
                          break;
                        }
                      }
                    }

                    // Antes de passar para os objetos filhos em "esquema de �rvore". Precisamos completar o DAOMap, isso pq quando ele � feito limitamos o mapeamento de estruturas hierarquicas por tender ao infinito. Vamos duplicando o mapeamento aqui, dinamicamente
                    String destPath = RUReflex.addPath(path, field.getName());
                    // System.out.println(dumpDAOMap(daoMap));
                    // Se n�o tivermos o caminho temos de completar dianimicamente no DAOMap
                    daoMap.createMapTableForCompositionTree(path, destPath, "id", getMetaRelationColumnMapped(field, ann));
                    // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composi��o n�o podem ter ID definido antes do pr�prio pai, provavelmente isso � um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composi��o novo (em insert).
                    persist(ds, daoMap, (isNew || itemVO.getId() == null), itemVO, itemVOOrig, destPath, persistedCache, sColumn, countIndex, updatePendings, dialect);

                    countIndex++;
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  Map hash = (Map) fieldValue;
                  Map hashOriginal = null;
                  if (entityVOOrig != null) hashOriginal = (Map) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                  for (Object key : hash.keySet()) {
                    VO itemVO = (VO) hash.get(key);
                    VO itemVOOrig = null;
                    if (hashOriginal != null) itemVOOrig = (VO) hashOriginal.get(key);
                    // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composi��o n�o podem ter ID definido antes do pr�prio pai, provavelmente isso � um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composi��o novo (em insert).
                    persist(ds, daoMap, (isNew || itemVO.getId() == null), itemVO, itemVOOrig, RUReflex.addPath(path, field.getName()), persistedCache, null, 0, updatePendings, dialect);
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case INNER_ASSOCIATION:
              // Neste caso n�o h� nada para fazer neste ponto.
              break;
            case PARENT_ASSOCIATION:
              // No caso de um relacionamento com o objeto pai, n�o temos de fazer nada, pois tanto o pai quando o ID do pai j� deve ter sido persistido
              break;
            case MANY_TO_MANY: {
              // Os relacionamentos ManyToMany precisam ter os inserts da tabela de Join realizados para "linkar" os dois objetos
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  for (Object item : (List) fieldValue) {
                    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createManyToManySelectStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) item, dialect); ResultSet rs = stmt.executeQuery()) {
                      if (!rs.next()) {
                        // Se n�o tem um resultado pr�ximo, criamos a inser��o, se n�o deixa quieto que j� foi feito
                        try (PreparedStatement stmt2 = DAOMap.createManyToManyInsertStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) item, dialect)) {
                          stmt2.executeUpdate();
                        }
                      }
                    } catch (Throwable e) {
                      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
                    }
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  for (Object item : ((Map) fieldValue).values()) {
                    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createManyToManySelectStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) item, dialect); ResultSet rs = stmt.executeQuery()) {
                      if (!rs.next()) {
                        // Se n�o tem um resultado pr�ximo, criamos a inser��o, se n�o deixa quieto que j� foi feito
                        try (PreparedStatement stmt2 = DAOMap.createManyToManyInsertStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) item, dialect)) {
                          stmt2.executeUpdate();
                        }
                      }
                    } catch (Throwable e) {
                      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
                    }
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
              // Se existir uma lista no objeto original, precisamos apagar todos os mapeamentos que n�o existem mais, caso contr�rio as desassocia��es n�o deixar�o de existir
              if (entityVOOrig != null) {
                final Object fieldValueOrig = RUReflex.getPropertyValue(entityVOOrig, field.getName());
                if (fieldValueOrig != null) {
                  if (List.class.isAssignableFrom(fieldValueOrig.getClass())) {
                    List listOrig = (List) fieldValueOrig;
                    for (Object itemOrig : listOrig) {
                      boolean found = false;
                      // Vamos iterar a lista de objetos atual para ver se encontramos o objeto. se n�o encontrar excluimos o link entre os objetos
                      if (fieldValue != null) {
                        for (Object item : (List) fieldValue) {
                          if (((VO) itemOrig).getId().equals(((VO) item).getId())) {
                            found = true;
                            break;
                          }
                        }
                      }
                      if (!found) {
                        try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createManyToManyDeleteStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) itemOrig, dialect)) {
                          stmt.executeUpdate();
                        } catch (Throwable e) {
                          throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
                        }
                      }
                    }
                  } else if (Map.class.isAssignableFrom(fieldValueOrig.getClass())) {
                    Map hashOrig = (Map) fieldValueOrig;
                    for (Object itemOrig : hashOrig.values()) {
                      boolean found = false;
                      // Vamos iterar a lista de objetos atual para ver se encontramos o objeto. se n�o encontrar excluimos o link entre os objetos
                      if (fieldValue != null) {
                        for (Object item : ((Map) fieldValue).values()) {
                          if (((VO) itemOrig).getId().equals(((VO) item).getId())) {
                            found = true;
                            break;
                          }
                        }
                      }
                      if (!found) {
                        try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createManyToManyDeleteStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) itemOrig, dialect)) {
                          stmt.executeUpdate();
                        } catch (Throwable e) {
                          throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
                        }
                      }
                    }
                  } else {
                    throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. N�o � poss�vel persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValueOrig.getClass().getCanonicalName() });
                  }
                }
              }
            }
              break;
          }
        }
        final RFWMetaCollectionField colAnn = field.getAnnotation(RFWMetaCollectionField.class);
        if (colAnn != null) {
          // Se temos uma collection para persistir, vamos iterar cada um dos itens e persisti-lo na tabela agora que certezamente temos um ID no objeto pai
          Object colValue = RUReflex.getPropertyValue(entityVO, field.getName());
          if (colValue != null) {
            if (colValue instanceof List<?>) {
              if (((List<?>) colValue).size() > 0) insertCollection(ds, daoMap, "@" + RUReflex.addPath(path, field.getName()), (List<?>) colValue, entityVO.getId(), dialect, colAnn);
            } else if (colValue instanceof HashSet<?>) {
              if (((HashSet<?>) colValue).size() > 0) insertCollection(ds, daoMap, "@" + RUReflex.addPath(path, field.getName()), new LinkedList<Object>((HashSet<?>) colValue), entityVO.getId(), dialect, colAnn);
            } else if (colValue instanceof Map<?, ?>) {
              if (((Map<?, ?>) colValue).size() > 0) insertCollection(ds, daoMap, "@" + RUReflex.addPath(path, field.getName()), new LinkedList<Object>(((Map<?, ?>) colValue).entrySet()), entityVO.getId(), dialect, colAnn);
            } else {
              throw new RFWCriticalException("O RFWDAO n�o sabe persistir uma RFWMetaCollectionField com o objeto do tipo '" + colValue.getClass().getCanonicalName() + "'");
            }
          }
        }
      }
    }

  }

  /**
   * Recupera a defini��o do atributo "column" da Annotation {@link RFWMetaRelationshipField}, com a possibilidade de interven��o do {@link DAOResolver}.
   *
   * @param field Field do objeto que estamos tratando, e procurando o valor da coluna para terminar o mapeamento.
   * @param ann refer�ncia para a {@link RFWMetaRelationshipField} encontrada.
   * @return Retorna o nome da coluna a ser utilizada no mapeamento.
   * @throws RFWException
   */
  private String getMetaRelationJoinTable(Field field, final RFWMetaRelationshipField ann) throws RFWException {
    String joinTable = null;
    if (this.resolver != null) {
      joinTable = this.resolver.getMetaRelationJoinTable(field, ann);
    }
    if (joinTable == null) {
      joinTable = ann.joinTable();
    }
    return joinTable;
  }

  /**
   * Recupera a defini��o do atributo "column" da Annotation {@link RFWMetaRelationshipField}, com a possibilidade de interven��o do {@link DAOResolver}.
   *
   * @param field Field do objeto que estamos tratando, e procurando o valor da coluna para terminar o mapeamento.
   * @param ann refer�ncia para a {@link RFWMetaRelationshipField} encontrada.
   * @return Retorna o nome da coluna a ser utilizada no mapeamento.
   * @throws RFWException
   */
  private String getMetaRelationColumn(Field field, final RFWMetaRelationshipField ann) throws RFWException {
    String column = null;
    if (this.resolver != null) {
      column = this.resolver.getMetaRelationColumn(field, ann);
    }
    if (column == null) {
      column = ann.column();
    }
    return column;
  }

  /**
   * Recupera a defini��o do atributo "columnMapped" da Annotation {@link RFWMetaRelationshipField}, com a possibilidade de interven��o do {@link DAOResolver}.
   *
   * @param field Field do objeto que estamos tratando, e procurando o valor da coluna para terminar o mapeamento.
   * @param ann refer�ncia para a {@link RFWMetaRelationshipField} encontrada.
   * @return Retorna o nome da coluna a ser utilizada no mapeamento.
   * @throws RFWException
   */
  private String getMetaRelationColumnMapped(Field field, final RFWMetaRelationshipField ann) throws RFWException {
    String column = null;
    if (this.resolver != null) {
      column = this.resolver.getMetaRelationColumnMapped(field, ann);
    }
    if (column == null) {
      column = ann.columnMapped();
    }
    return column;
  }

  /**
   * Atualiza uma associa��o quando a coluna de FK est� na tabela do objento associado, e n�o na tabela da entidade sendo atualizada.
   *
   * @param ds Data Source da conex�o.
   * @param map Mapeamento Objeto x Tabelas
   * @param path Caminho completo do atributo que contem o objeto associado.
   * @param id ID do objeto associado (para identifica��o na tabela que ser� atualizada).
   * @param newID ID que ser� colocado na tabela. Normalmente o ID do objeto sendo editado pelo RFWDAO ou null caso estejamos eliminando a associa��o.
   * @throws RFWException
   */
  private static <VO extends RFWVO> void updateExternalFK(DataSource ds, DAOMap map, String path, Long id, Long newID, SQLDialect dialect) throws RFWException {
    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createUpdateExternalFKStatement(conn, map, path, id, newID, dialect)) {
      stmt.executeUpdate();
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao excluir os elementos de uma Collection no banco de dados!", e);
    }
  }

  /**
   * Atualiza uma associa��o quando a coluna de FK est� na tabela do pr�prio objeto (sem alterar mais dada do objeto.
   *
   * @param ds Data Source da conex�o.
   * @param map Mapeamento Objeto x Tabelas
   * @param path Caminho completo at� o objeto que ser� atualizado.
   * @param property Propriedade do objeto que tem a associa��o com a FK na pr�pria tabela.
   * @param id ID do objeto (para identifica��o na tabela que ser� atualizada).
   * @param newID ID que ser� colocado na tabela. Normalmente o ID do objeto sendo editado pelo RFWDAO ou null caso estejamos eliminando a associa��o.
   * @throws RFWException
   */
  private static <VO extends RFWVO> void updateInternalFK(DataSource ds, DAOMap map, String path, String property, Long id, Long newID, SQLDialect dialect) throws RFWException {
    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createUpdateInternalFKStatement(conn, map, path, property, id, newID, dialect)) {
      stmt.executeUpdate();
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao corrigir FK de associa��o do objeto no banco de dados!", e);
    }
  }

  /**
   * Este m�todo � utilizado para excluir do banco todos os elementos de um atributo anotado com a {@link RFWMetaCollectionField}.
   */
  private static <VO extends RFWVO> void deleteCollection(DataSource ds, DAOMap map, VO vo, String path, SQLDialect dialect) throws RFWException {
    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createDeleteCollectionStatement(conn, map, path, vo.getId(), dialect)) {
      stmt.executeUpdate();
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao excluir os elementos de uma Collection no banco de dados!", e);
    }
  }

  private static <VO extends RFWVO> void insertCollection(DataSource ds, DAOMap map, String path, List<?> items, Long parentID, SQLDialect dialect, RFWMetaCollectionField ann) throws RFWException {
    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createInsertCollectionStatement(conn, map, path, items, parentID, dialect, ann)) {
      stmt.executeUpdate();
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao inserir os elementos de uma Collection no banco de dados!", e);
    }
  }

  private static <VO extends RFWVO> void delete(DataSource ds, DAOMap map, VO vo, String path, SQLDialect dialect) throws RFWException {
    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createDeleteStatement(conn, map, path, dialect, vo.getId())) {
      stmt.executeUpdate();
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
  }

  /**
   * Busca a entidade a partir do seu ID.
   *
   * @param id ID do objeto a ser encontrado no banco de dados.
   * @param attributes Atributos da entidade que devem ser recuperados.
   * @return Objeto montado caso seja encontrado, null caso contr�rio.
   * @throws RFWException Lan�ado caso ocorra algum problema para montar ou obter o objeto
   */
  @SuppressWarnings("unchecked")
  public VO findById(Long id, String[] attributes) throws RFWException {
    if (id == null) throw new NullPointerException("ID can't be null!");

    final DAOMap map = createDAOMap(this.type, attributes);

    // Cria a condi��o do ID
    RFWMO mo = new RFWMO();
    mo.equal("id", id);

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, attributes, true, mo, null, null, null, null, dialect); ResultSet rs = stmt.executeQuery()) {
      final List<RFWVO> list = mountVO(rs, map, null);
      if (list.size() > 1) {
        throw new RFWCriticalException("Encontrado mais de um objeto em uma busca por ID.", new String[] { "" + id, RUArray.concatArrayIntoString(attributes, ",") });
      } else if (list.size() == 1) {
        return (VO) list.get(0);
      }
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
    return null;
  }

  /**
   * Busca a entidade a partir do seu ID para Atualiza��o.
   *
   * @param id ID do objeto a ser encontrado no banco de dados.
   * @param attributes Atributos da entidade que devem ser recuperados. Atributos de associa��o e composi��o s�o recuperados automaticamente.
   * @return Objeto montado caso seja encontrado, null caso contr�rio.
   * @throws RFWException Lan�ado caso ocorra algum problema para montar ou obter o objeto
   */
  @SuppressWarnings({ "deprecation", "unchecked" }) // attributes = new String[0];
  public VO findForUpdate(Long id, String[] attributes) throws RFWException {
    if (id == null) throw new NullPointerException("ID can't be null!");

    final String[] attForUpdate = RUReflex.getRFWVOUpdateAttributes(type);
    attributes = RUArray.concatAll(new String[0], attributes, attForUpdate);

    final DAOMap map = createDAOMap(this.type, attributes);

    // Cria a condi��o do ID
    RFWMO mo = new RFWMO();
    mo.equal("id", id);

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, attributes, true, mo, null, null, null, null, dialect); ResultSet rs = stmt.executeQuery()) {
      final List<RFWVO> list = mountVO(rs, map, null);
      if (list.size() > 1) {
        throw new RFWCriticalException("Encontrado mais de um objeto em uma busca por ID.", new String[] { "" + id, RUArray.concatArrayIntoString(attributes, ",") });
      } else if (list.size() == 1) {
        final VO v = (VO) list.get(0);
        v.setFullLoaded(true);
        return v;
      }
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
    return null;
  }

  /**
   * Busca uma lista IDs dos VOs baseado em um crit�rio de "search".
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param orderBy Objeto para definir a ordena��o da lista
   * @return Lista com os objetos que respeitam o crit�rio estabelecido e na ordem desejada.
   * @throws RFWException Lan�ado em caso de erro.
   */
  public List<Long> findIDs(RFWMO mo, RFWOrderBy orderBy) throws RFWException {
    return findIDs(mo, orderBy, null, null);
  }

  /**
   * Busca uma lista IDs dos VOs baseado em um crit�rio de "search".
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param orderBy Objeto para definir a ordena��o da lista
   * @param offSet Define quantos registros a partir do come�o devemos pular (n�o retornar), por exemplo, um offSet de 0, retorna desde o primeiro (index 0).
   * @param limit Define quantos registros devemos retornar da lista. Define a quantidade e n�o o index como o "offSet". Se ambos forem combinados, ex: offset = 5 e limit = 10, retorna os registros desde o index 5 at� o idnex 15, ou seja da 6� linha at� a 15�.
   * @return Lista com os objetos que respeitam o crit�rio estabelecido e na ordem desejada.
   * @throws RFWException Lan�ado em caso de erro.
   */
  public List<Long> findIDs(RFWMO mo, RFWOrderBy orderBy, Integer offSet, Integer limit) throws RFWException {
    // Primeiro vamos buscar apenas os ids do objeto raiz que satisfazem as condi��es
    String[] moAtt = new String[0];
    if (mo != null) moAtt = RUArray.concatAll(moAtt, mo.getAttributes().toArray(new String[0]));
    if (orderBy != null) moAtt = RUArray.concatAll(moAtt, orderBy.getAttributes().toArray(new String[0]));

    final DAOMap map = createDAOMap(this.type, moAtt);

    try (Connection conn = ds.getConnection()) {
      // conn.setAutoCommit(false);
      try (PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, new String[] { "id" }, false, mo, orderBy, offSet, limit, null, dialect)) {
        stmt.setFetchSize(1000);
        try (ResultSet rs = stmt.executeQuery()) {
          // Ao usar o LinkedHashSet ele n�o aceita valores iguais (ele os sobrep�e automaticamente na Hash) fazendo com que no final s� tenhamos uma lista de objetos distintos.
          // Precisamos utilizar o LinkedHashSet ao inv�s do habitual HashSet para que ele mantenha a ordem dos objetos na sa�da.
          final LinkedHashSet<Long> ids = new LinkedHashSet<>();
          DAOMapTable mTable = map.getMapTableByPath("");
          while (rs.next())
            ids.add(getRSLong(rs, mTable.schema, mTable.table, mTable.alias, "id", dialect)); // ids.add(rs.getLong("id"));

          if (dialect == SQLDialect.DerbyDB) conn.commit(); // Derby Exisge o commit

          ArrayList<Long> list = new ArrayList<Long>(ids);
          return list;
        }
      }
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
  }

  /**
   * Busca a quantidade de itens que uma busca por filtro {@link RFWMO} retornar�.<br>
   * Esta busca faz uma query do tipo 'SELECT COUNT(*)...' trazendo do banco apenas o total, sem carregar qualquer outro tipo de objeto, o que melhora a performance quando o desejado � apenas o total de itens.
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @return Long com a quantidade de itens encontrados. Zero se a query retornou vazia.
   * @throws RFWException Lana�ado em caso de erro.
   */
  public Long count(RFWMO mo) throws RFWException {
    RFWField[] fields = new RFWField[1];
    fields[0] = RFWField.count();

    List<Object[]> list = findListEspecial(fields, mo, null, null, null, null);
    return (Long) list.get(0)[0];
  }

  /**
   * Busca os valores distintos de uma determinada coluna do banco de dados, com a possibilidade de filtrar os registros com o {@link RFWMO}.<Br>
   * Esta consulta usa diretamente o DISTINCT no banco de dados na coluna desejada.
   *
   * @param attribute nome do atributo (ou caminho) para realizar a consulta de valores distintos no banco de dados.
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @return Lista com os valores distintos da coluna/propriedade solicitada. Os objetos tendem a ser equivalente ao tipo de dado no banco de dados. Pois s�o criados a partir do m�todo .getObject() do ResultSet, e sem interven��o do RFWDAO.
   * @throws RFWException
   */
  public List<Object> findDistinct(String attribute, RFWMO mo) throws RFWException {
    RFWField[] fields = new RFWField[1];
    fields[0] = RFWField.distinct(attribute);

    List<Object[]> list = findListEspecial(fields, mo, null, null, null, null);

    List<Object> resultList = new ArrayList<>();
    for (Object[] array : list) {
      if (array != null && array.length > 0) {
        resultList.add(array[0]);
      }
    }
    return resultList;
  }

  /**
   * Busca uma lista de VOs baseado em um crit�rio de "search".
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param orderBy Objeto para definir a ordena��o da lista
   * @param attributes Atributos que devem ser recuperados em cada objeto.
   * @return Lista com os objetos que respeitam o crit�rio estabelecido e na ordem desejada.
   * @throws RFWException Lan�ado em caso de erro.
   */
  public List<VO> findList(RFWMO mo, RFWOrderBy orderBy, String[] attributes) throws RFWException {
    return findList(mo, orderBy, attributes, null, null);
  }

  /**
   * Busca uma lista de VOs baseado em um crit�rio de "search".
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param orderBy Objeto para definir a ordena��o da lista
   * @param attributes Atributos que devem ser recuperados em cada objeto.
   * @param offSet Define quantos registros a partir do come�o devemos pular (n�o retornar), por exemplo, um offSet de 0, retorna desde o primeiro (index 0).
   * @param limit Define quantos registros devemos retornar da lista. Define a quantidade e n�o o index como o "offSet". Se ambos forem combinados, ex: offset = 5 e limit = 10, retorna os registros desde o index 5 at� o idnex 15, ou seja da 6� linha at� a 15�.
   * @return Lista com os objetos que respeitam o crit�rio estabelecido e na ordem desejada.
   * @throws RFWException Lan�ado em caso de erro.
   */
  @SuppressWarnings("unchecked")
  public List<VO> findList(RFWMO mo, RFWOrderBy orderBy, String[] attributes, Integer offSet, Integer limit) throws RFWException {
    if (mo == null) mo = new RFWMO();
    // Primeiro vamos buscar apenas os ids do objeto raiz que satisfazem as condi��es
    String[] atts = RUArray.concatAll(new String[0], mo.getAttributes().toArray(new String[0]), attributes);
    if (orderBy != null) atts = RUArray.concatAll(atts, orderBy.getAttributes().toArray(new String[0]));
    final DAOMap map = createDAOMap(this.type, atts);

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, new String[] { "id" }, false, mo, orderBy, offSet, limit, null, dialect); ResultSet rs = stmt.executeQuery()) {
      final LinkedList<Long> ids = new LinkedList<>();
      DAOMapTable mTable = map.getMapTableByPath("");
      while (rs.next()) {
        final long id = getRSInteger(rs, mTable.schema, mTable.table, mTable.alias, "id", dialect);// rs.getLong("id");
        if (!ids.contains(id)) ids.add(id); // N�o permite colocar duplicado, dependendo das conex�es utilizadas nos LeftJoins, o mesmo ID pode retornar m�ltiplas vezes
      }

      // Se n�o temos um ID para procurar, � pq o objeto n�o foi encontrado, simplesmente retorna a lista vazia
      if (ids.size() == 0) return new LinkedList<>();

      // Com base nos IDs retornados, montar um RFWMO para retornar todos os objetos com os IDs, e neste caso j� passamos as colunas que queremos montar no objeto
      RFWMO moIDs = new RFWMO();
      moIDs.in("id", ids);

      // Refazemos apenas os atributos que queremos selecionar e os do OrderBy.
      if (orderBy != null) {
        if (attributes == null) {
          atts = orderBy.getAttributes().toArray(new String[0]);
        } else {
          atts = RUArray.concatAll(attributes, orderBy.getAttributes().toArray(new String[0]));
        }
      } else {
        atts = attributes;
      }
      try (PreparedStatement stmt2 = DAOMap.createSelectStatement(conn, map, atts, true, moIDs, orderBy, offSet, limit, null, dialect); ResultSet rs2 = stmt2.executeQuery()) {
        List<VO> list = (List<VO>) mountVO(rs2, map, null);
        return list;
      }
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
  }

  public List<Object[]> findListEspecial(RFWField[] fields, RFWMO mo, RFWOrderBy orderBy, RFWField[] groupBy, Integer offSet, Integer limit) throws RFWException {
    return findListEspecial(fields, mo, orderBy, groupBy, offSet, limit, null);
  }

  public List<Object[]> findListEspecial(RFWField[] fields, RFWMO mo, RFWOrderBy orderBy, RFWField[] groupBy, Integer offSet, Integer limit, Boolean useFullJoin) throws RFWException {
    if (mo == null) mo = new RFWMO();

    // Verificamos todos os atributos que precisamos mapear conforme utiliza��o em cada parte do SQL
    String[] attsMO = mo.getAttributes().toArray(new String[0]);
    String[] attsFields = new String[0];
    if (fields == null) throw new RFWCriticalException("O par�metro 'fields' n�o pode ser nulo!");
    for (RFWField field : fields) {
      attsFields = RUArray.concatAll(attsFields, field.getAttributes().toArray(new String[0]));
    }
    String[] attsGroupBy = new String[0];
    if (groupBy != null) for (RFWField field : groupBy) {
      attsGroupBy = RUArray.concatAll(attsGroupBy, field.getAttributes().toArray(new String[0]));
    }
    String[] attsOrderBy = new String[0];
    if (orderBy != null) attsOrderBy = orderBy.getAttributes().toArray(new String[0]);

    String[] attsTotal = RUArray.concatAll(attsMO, attsFields, attsOrderBy, attsGroupBy);

    // Mapeamos todos os objetos necess�rios
    final DAOMap map = createDAOMap(this.type, attsTotal);

    try (Connection conn = ds.getConnection(); PreparedStatement stmt2 = DAOMap.createSelectStatement(conn, map, fields, mo, orderBy, groupBy, offSet, limit, useFullJoin, dialect); ResultSet rs2 = stmt2.executeQuery()) {
      List<Object[]> list = new LinkedList<Object[]>();
      while (rs2.next()) {
        Object[] row = new Object[fields.length];
        for (int i = 0; i < row.length; i++) {
          row[i] = rs2.getObject(i + 1);
        }
        list.add(row);
      }
      return list;
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
  }

  /**
   * Busca um objeto atrav�s do MatchObject. Note que as condi��es usadas no MO devem garantir que apenas 1 objeto ser� retornado, como a busca por um campo definido como Unique. <br>
   * Caso este m�todo encontre mais de um objeto para a mesma busca, um erro cr�tico ser� lan�ado.
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param attributes Atributos que devem ser recuperados em cada objeto.
   * @return Objecto �nico encontrado
   * @throws RFWException Lan�ado em caso de erro.
   */
  public VO findUniqueMatch(RFWMO mo, String[] attributes) throws RFWException {
    if (mo == null) mo = new RFWMO();
    // Primeiro vamos buscar apenas os ids do objeto raiz que satisfazem as condi��es
    final DAOMap map = createDAOMap(this.type, RUArray.concatAll(new String[0], mo.getAttributes().toArray(new String[0]), attributes));

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, new String[] { "id" }, false, mo, null, null, null, null, dialect); ResultSet rs = stmt.executeQuery()) {

      DAOMapTable mTable = map.getMapTableByPath("");
      Long id = null;
      while (rs.next()) {
        final long rsID = getRSLong(rs, mTable.schema, mTable.table, mTable.alias, "id", dialect); // rs.getLong("id");
        // Precisamos verficiar se n�o � o mesmo ID pq as vezes a consult inclui joins de listas, o que faz com que v�rias linhas retornem para o mesmo objeto
        if (id != null && id != rsID) throw new RFWCriticalException("Encontrado mais de um objeto pelo m�todo 'findUniqueMatch()'.");
        id = rsID;
      }

      // Se n�o temos um ID para procurar, � pq o objeto n�o foi encontrado, simplesmente retorna null
      if (id == null) return null;
      // Com base nos IDs retornados, montar um RFWMO para retornar todos os objetos com os IDs, e neste caso j� passamos as colunas que queremos montar no objeto
      return findById(id, attributes);
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
  }

  /**
   * Busca um objeto atrav�s do MatchObject para edi��o. Note que as condi��es usadas no MO devem garantir que apenas 1 objeto ser� retornado, como a busca por um campo definido como Unique. <br>
   * Caso este m�todo encontre mais de um objeto para a mesma busca, um erro cr�tico ser� lan�ado.
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param attributes Atributos que devem ser recuperados em cada objeto.
   * @return Objecto �nico encontrado
   * @throws RFWException Lan�ado em caso de erro.
   */
  public VO findUniqueMatchForUpdate(RFWMO mo, String[] attributes) throws RFWException {
    if (mo == null) mo = new RFWMO();
    // Primeiro vamos buscar apenas os ids do objeto raiz que satisfazem as condi��es
    final DAOMap map = createDAOMap(this.type, RUArray.concatAll(new String[0], mo.getAttributes().toArray(new String[0]), attributes));

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, new String[] { "id" }, false, mo, null, null, null, null, dialect); ResultSet rs = stmt.executeQuery()) {
      DAOMapTable mTable = map.getMapTableByPath("");
      Long id = null;
      while (rs.next()) {
        final long rsID = getRSLong(rs, mTable.schema, mTable.table, mTable.alias, "id", dialect); // rs.getLong("id");
        // Precisamos verficiar se n�o � o mesmo ID pq as vezes a consult inclui joins de listas, o que faz com que v�rias linhas retornem para o mesmo objeto
        if (id != null && id != rsID) throw new RFWCriticalException("Encontrado mais de um objeto pelo m�todo 'findUniqueMatch()'.");
        id = rsID;
      }

      // Se n�o temos um ID para procurar, � pq o objeto n�o foi encontrado, simplesmente retorna null
      if (id == null) return null;
      // Com base nos IDs retornados, montar um RFWMO para retornar todos os objetos com os IDs, e neste caso j� passamos as colunas que queremos montar no objeto
      return findForUpdate(id, attributes);
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
  }

  /**
   * M�todo utilizado ler os dados do result set e montar os objetos conforme forem retornados.
   *
   * @param rs ResultSet da consulta no banco de dados
   * @param map mapeamento da consulta.
   * @param cache Cache com os objetos j� criados. Utilizado para a recurs�o do m�todo. Quando chamado de fora do pr�prio m�todo: passar NULL.
   * @throws RFWException
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private List<RFWVO> mountVO(ResultSet rs, DAOMap map, HashMap<String, RFWVO> cache) throws RFWException {
    try {
      // ATEN��O: N�O TRABALHAMOS COM LinkedList, apesar de ter menos overhead que o ArrayList pq o Glassfish ao tentar colocar o linkedlist na fachada acaba estourando a mem�ria. � um BUG do Glassfish que n�o clona direito o objeto.
      // Ao usar o ArrayList o Glassfish funciona melhor. Ou seja, para passar pela fachada o Arraylist � mais r�pido e evita stackoverflow de "copyObject"
      final ArrayList<RFWVO> vos = new ArrayList<>();

      // Lista de listas de obejtos que precisam ser "limpas"
      // Quando as listas tem um sortIndex definido, o banco envia os objetos conforme o orderBy definido do banco e n�o como deveriamos ter na lista.
      // Como n�o temos o sortIndex salvo no VO, o valor ser� jogado fora com o ResultSet, n�s criamos objetos "dummies" para ocupar a posi��o informada pelo indexOrder at� que o objeto correto seja recuperado.
      // PROBLEMAS: se por algum motivo o sortIndex ficou errado, digamos faltando o elemento 4 de um total de 10, por exemplo pq o mapeamento foi apagado por ON DELETE CASCADE do banco,
      // a lista acaba sendo retornada com o dummie objetct ocupando a posi��o do item que sumiu. Por isso salvamos aqui todas as listas que tiveram dummies objetos colocados, para que no fim da montagem possamos limpar essas listas e manter a ordem desejada
      final HashSet<List<?>> cleanLists = new HashSet<List<?>>();

      // Cache com os objetos j� criados, assim reaproveitamos ao inv�s de criar v�rias inst�ncias do mesmo objeto.
      final HashMap<String, RFWVO> objCache;
      if (cache == null) {
        objCache = new HashMap<>();
      } else {
        objCache = cache;
      }

      while (rs.next()) {
        final HashMap<String, RFWVO> aliasCache = new HashMap<>(); // este cache armazena os objetos desse ResultSet. Sendo que a chave da Hash � o Alias utilizado no SQL para representar o objeto/tabela.

        // Vamos iterar cada tabela para criar seus objetos principais
        for (DAOMapTable mTable : map.getMapTable()) {
          if (mTable.path.startsWith("@")) { // Tabelas de Collection (RFWMetaCollection)
            // Verifica se temos a coluna ID no resultSet, isso indica que a tabela do objeto foi recuperada
            boolean retrived = false;
            try {
              getRSLong(rs, mTable.schema, mTable.table, mTable.alias, mTable.column, dialect); // rs.getLong(mTable.alias + "." + mTable.column);
              // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
              // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
              retrived = true;
            } catch (RFWException e) {
              if (e.getCause() != null && e.getCause() instanceof SQLException) {
                // um SQLException indica que a coluna n�o est� presente, provavelmente pq os objetos n�o foram exigidos. Neste caso pulamos este objeto.
              } else {
                throw e;
              }
            }

            if (retrived) {
              // Busca o objeto 'pai', que tem a collection, pelo ID definido na foreingKey
              DAOMapTable joinTable = map.getMapTableByAlias(mTable.joinAlias);
              Long parentID = getRSLong(rs, joinTable.schema, joinTable.table, joinTable.alias, "id", dialect); // Long parentID = rs.getLong(mTable.joinAlias + ".id");
              if (rs.wasNull()) parentID = null;

              // Se n�o temos um ID do Pai, n�o temos nem um objeto para incializar
              if (parentID != null) {
                final DAOMapTable parentTable = joinTable;
                final String key = parentTable.type.getCanonicalName() + "." + parentID;
                RFWVO vo = objCache.get(key);

                // Se temos a chave da FK mas n�o encontramos o objeto, temos um probema... Se n�o temos a FK � pq provavelmente j� temos o objeto pai nulo tamb�m...
                if (vo == null) throw new RFWCriticalException("N�o foi poss�vel encontrar o pai para colocar os valores da RFWMetaCollection!", new String[] { mTable.table, mTable.column });

                // Recupera o field com o valor baseado no atributo do Path da Tabela. Note que o Path da tabela tem o @ no in�cio para identificar que � uma tabela de MetaCollection. J� os campos, tem o @ no fim do nome do field, por isso a opera��o remover a @ do come�o e concatena-la no final.
                final DAOMapField mField = map.getMapFieldByPath(mTable.path.substring(1) + "@");

                final RFWMetaCollectionField ann = (RFWMetaCollectionField) RUReflex.getRFWMetaAnnotation(vo.getClass(), mField.field.substring(0, mField.field.length() - 1));
                if (ann.targetRelationship() == null) throw new RFWCriticalException("N�o foi poss�vel encontrar o TargetRelationship da MetaCollection em '" + vo.getClass().getCanonicalName() + "' do m�todo '" + mField.field.substring(0, mField.field.length() - 1) + "'.");

                // Recupera o conte�do a ser colocado na MetaCollection
                Object content = null;
                try {
                  if (String.class.isAssignableFrom(ann.targetRelationship())) {
                    content = getRSString(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // content = rs.getString(mTable.alias + "." + mField.column);
                    if (rs.wasNull()) content = null;
                  } else if (BigDecimal.class.isAssignableFrom(ann.targetRelationship())) {
                    content = getRSBigDecimal(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect);
                  } else if (Enum.class.isAssignableFrom(ann.targetRelationship())) {
                    content = getRSString(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // content = rs.getString(mTable.alias + "." + mField.column);
                    if (rs.wasNull()) {
                      content = null;
                    } else {
                      content = Enum.valueOf((Class<Enum>) ann.targetRelationship(), (String) content);
                    }
                  } else {
                    throw new RFWCriticalException("RFWDAO n�o preparado para tratar Collections com target do tipo '" + ann.targetRelationship().getCanonicalName() + "'!");
                  }
                } catch (SQLException e) {
                  // Uma SQLException ao tentar recuperar a coluna, indica que a coluna n�o foi "solicitada", assim n�o temos nada para adicionar, nem o objeto Hash
                }

                if (content != null) {
                  // Com o VO em m�os, verificamos o tipo de MetaCollection que temos na entidade (Map ou List) para saber como popular e instanciar se ainda for o primeiro
                  final Class<?> rt = RUReflex.getPropertyTypeByType(parentTable.type, mField.field.substring(0, mField.field.length() - 1)); // Remove a @ do final do Field
                  if (List.class.isAssignableFrom(rt)) {
                    // Se � um List procuramos a coluna de 'sort' para saber como organizar os itens
                    DAOMapField sortField = map.getMapFieldByPath(mTable.path.substring(1) + "@sortColumn");
                    Integer sortIndex = null; // A coluna de organiza��o n�o � obrigat�ria para montar uma lista, s� deixamos de garantir a ordem.
                    if (sortField != null) {
                      sortIndex = getRSInteger(rs, mTable.schema, mTable.table, mTable.alias, sortField.column, dialect);
                      // sortIndex = rs.getInt(mTable.alias + "." + sortField.column);
                    }

                    List list = (List) RUReflex.getPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1));
                    if (list == null) {
                      list = new ArrayList<>();
                    }

                    if (!list.contains(content)) {
                      if (sortIndex == null) {
                        // Se n�o temos sortIndex, simplesmente adicionamos � lista a medida que vamos recuperando
                        list.add(content);
                      } else {
                        if (list.size() == 0) cleanLists.add(list); // Se � nova, e temos indexa��o, inclu�mos a lista para limpeza depois.
                        // Se temos um index, temos de repeita-lo e ir populando a lista corretamente. Como podemos receber a lista fora de ordem temos de verificar o tamanho da lista antes de inserir o objeto. Caso a lista ainda seja menor do que a posi��o a ser ocupada, inclu�mos alguns "Dummy" Objects para crescer a lista e j� deixar o objeto na posi��o correta
                        while (list.size() < sortIndex + 1)
                          list.add(map); // incluimos um objeto que j� existe para evitar instanciar novos objetos na mem�ria. E Adicionar NULL n�o funciona na Linked List
                        // Inclu�mos inclusive um objeto no lugar onde nosso objeto ser� colocado propositalmente, assim n�o precisamos ter duas l�gicas abaixo
                        list.add(sortIndex, content); // Ao incluir o objeto no �ndex determinado, empurramos todos os outros para frente, por isso temos de remover o pr�ximo objeto (que antes ocupava o lugar deste)
                        list.remove(sortIndex + 1);
                      }
                      RUReflex.setPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1), list, false);
                    }
                  } else if (HashSet.class.isAssignableFrom(rt)) {
                    HashSet set = (HashSet) RUReflex.getPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1));
                    if (set == null) {
                      set = new HashSet<>();
                    }
                    set.add(content); // n�o testamos se j� existe pq o SET n�o permite itens repetidos, ele substituir� automaticamente itens repetidos
                    RUReflex.setPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1), set, false);
                  } else if (Map.class.isAssignableFrom(rt)) {
                    // Se � um Map procuramos a coluna de 'key' para saber a chave que devemos incluir na Map
                    DAOMapField keyField = map.getMapFieldByPath(mTable.path.substring(1) + "@keyColumn");
                    Object keyValue = getRSString(rs, mTable.schema, mTable.table, mTable.alias, keyField.column, dialect); // rs.getString(mTable.alias + "." + keyField.column);

                    // Verifica a exist�ncia de um Converter para a chave de acesso
                    if (RFWDAOConverterInterface.class.isAssignableFrom(ann.keyConverterClass())) {
                      // Object ni = ann.keyConverterClass().newInstance();
                      Object ni = createNewInstance(ann.keyConverterClass());
                      if (!(ni instanceof RFWDAOConverterInterface)) throw new RFWCriticalException("A classe '${0}' definida no atributo '${1}' da classe '${2}' n�o � um RFWDAOConverterInterface v�lido!", new String[] { ann.keyConverterClass().getCanonicalName(), mField.field, vo.getClass().getCanonicalName() });
                      keyValue = ((RFWDAOConverterInterface) ni).toVO(keyValue);
                    }

                    Map hash = (Map) RUReflex.getPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1));
                    if (hash == null) {
                      hash = new LinkedHashMap<>();
                    }
                    // Recupera o atributo do objeto que � usado como chave da hash
                    if (!hash.containsKey(keyValue)) {
                      hash.put(keyValue, content); // S� adiciona se ainda n�o tiver este objeto
                      RUReflex.setPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1), hash, false);
                    }
                  } else {
                    throw new RFWCriticalException("O tipo ${0} n�o � suportado pela RFWMetaCollection! Atributo '${1}' da classe '${2}'.", new String[] { rt.getCanonicalName(), mField.field.substring(0, mField.field.length() - 1), parentTable.type.getCanonicalName() });
                  }
                }
              }
            }
          } else if (mTable.path.startsWith(".")) { // Tabelas de N:N (join Tables)
            // Ignora as tabelas de N:N, n�o faz nada!
          } else {
            // Verifica se temos a coluna ID no resultSet, isso indica que a tabela do objeto foi recuperada
            Long id = null;
            boolean retrived = false;
            try {
              id = getRSLong(rs, mTable.schema, mTable.table, mTable.alias, "id", dialect); // rs.getLong(mTable.alias + ".id");
              // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
              // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
              retrived = true;
            } catch (RFWException e) {
              if (e.getCause() != null && e.getCause() instanceof SQLException) {
                // um SQLException indica que a coluna n�o est� presente, provavelmente pq os objetos n�o foram exigidos. Neste caso pulamos este objeto.
              } else {
                throw e;
              }
            }
            if (id != null) {
              final String key = mTable.type.getCanonicalName() + "." + id;
              RFWVO vo = objCache.get(key);
              if (vo == null) {
                vo = (RFWVO) createNewInstance(mTable.type);
                vo.setId(id);
                objCache.put(key, vo);

                // Iteramos os campos em busca das informa��es deste VO
                for (DAOMapField mField : map.getMapField()) {
                  // Ignora o field ID pq ele n�o est� no objeto sendo escrito (� herdado do pai) e o m�todo write n�o o encontra. Sem contar que j� foi escrito diretamente acima sem a necessidade de reflex�o
                  if (mField.table == mTable && !"id".equals(mField.field)) writeToVO(mField, (VO) vo, rs);
                }
              }
              aliasCache.put(mTable.alias, vo);
              // Se � um objeto da tabela raiz, adicionamos tamb�m na lista que vamos retornar. N�o podemos ter essa linha s� quando criamos o objeto pq as vezes um objeto raiz � criado primeiro por ter sido apontado em um relacionamento c�clico. E embor ao objeto j� tenha sido criado em outra itera��o, ele deve ser adiconado na lista de objetos raiz s� agora.
              if ("".equals(mTable.path) && !vos.contains(vo)) vos.add(vo);

            } else if (retrived) {
              // Se n�o tem objeto, mas o procuramos, vamos incluir o objeto "nulo" na hash de cache para que fique explicito de que o procuramos o objeto, s� n�o existe nenhuma associa��o
              aliasCache.put(mTable.alias, null);
            }
          }
        }

        // Com todos os objetos criados, s� precisamos defini-los para montar a hierarquia
        for (int iterationControl = 0; iterationControl < 2; iterationControl++) {
          // O Loop do iteration control foi criado para garantir que as hashs s� ser�o montadas em uma segunda itera��o, depois que os demais objetos j� foram montados. Isso porqu� a chave da hash possa ter propriedades aninhadas em seus subobjetos, e garantir que eles j� foram montados:
          // - Itera��o 0: monta todos os objetos e os "relaciona" nas propriedades diretas e Listas
          // - Itera��o 1: monta as Hashs para que suas propriedaes aninhadas n�o estejam nulas.
          for (DAOMapTable mTable : map.getMapTable()) {
            if (mTable.joinAlias != null && !mTable.path.startsWith(".")) { // joinAlias != null -> ignora a tabela raiz, Todos os objetos ra�z n�o precisem ser colocados dentro de outro objeto, mas colocamos no array de objetos que vamos retornar | !mTable.path.startsWith(".") -> ignora as tabelas de N:N que n�o tem um objeto
              RFWVO vo = aliasCache.get(mTable.alias);
              // VO pode ser nulo caso a associa��o n�o exista
              if (vo != null) {
                RFWVO join = aliasCache.get(mTable.joinAlias);
                if (join == null) {
                  // Join � nulo quando temos entre este objeto e o objeto de joinAlias uma tabela de N:N. Neste caso temos de pular esse objeto
                  final DAOMapTable tmp = map.getMapTableByAlias(mTable.joinAlias);
                  // Se for mesmo a tabela de Join, ela tem o caminho come�ando com ".". Se n�o for uma tabela de join n�o utilizamos o objeto pois pode ser s� um caso de 2 Joins de objetos diferentes e estamos pulando 1
                  if (tmp.path.startsWith(".")) join = aliasCache.get(tmp.joinAlias);
                }
                final String relativePath = RUReflex.getLastPath(mTable.path); // pega o caminho em rala��o ao objeto atual, n�o desde a raiz
                final Class<?> rt = RUReflex.getPropertyTypeByType(join.getClass(), relativePath);
                if (RFWVO.class.isAssignableFrom(rt)) {
                  if (iterationControl == 0) {
                    RUReflex.setPropertyValue(join, relativePath, vo, false);
                  }
                } else if (List.class.isAssignableFrom(rt)) {
                  if (iterationControl == 0) {
                    List list = (List) RUReflex.getPropertyValue(join, relativePath);
                    if (list == null) {
                      list = new ArrayList<>();
                    }
                    if (!list.contains(vo)) { // S� adiciona se ainda n�o tiver este objeto
                      // Se � uma lista, verificamos se no atributo do VO temos a defini��o da coluna de 'sortColumn', para montar a lista na ordem correta.
                      final RFWMetaRelationshipField ann = join.getClass().getDeclaredField(relativePath).getAnnotation(RFWMetaRelationshipField.class);
                      Integer sortIndex = null;
                      if (ann != null && !"".equals(ann.sortColumn())) {
                        sortIndex = getRSInteger(rs, mTable.schema, mTable.table, mTable.alias, ann.sortColumn(), dialect); // rs.getInt(mTable.alias + "." + ann.sortColumn());
                      }
                      // Verificamos se � um caso de composi��o de �rvore
                      if (ann.relationship() == RelationshipTypes.COMPOSITION_TREE) {
                        // Nos casos de composi��o de �rvore vamos recber o primeiro objeto, mas n�o os objetos filhos da �rvore completa. Isso pq n�o teriamos como fazer infinitos JOINS no SQL para garantir que todos os objetos seriam retornados.
                        // Nestes casos vamos resolicitar ao DAO este objeto de forma completa, incluindo seu pr�ximo filho. Isso ser� feito de forma recursiva at� que todos sejam recuperados.
                        // Para aproveitar o mesmo cache de objetos, chamamos um m�todo espec�fico para isso, que criar� um SQL baseado no DAOMap que j� temos deste objeto e passando o cache de objetos
                        vo = fullFillCompositoinTreeObject(map, map.getMapTableByAlias(mTable.joinAlias), vo, objCache);
                      }
                      if (sortIndex == null) {
                        // Se n�o temos sortIndex, simplesmente adicionamos � lista a medida que vamos recuperando
                        list.add(vo);
                      } else {
                        if (list.size() == 0) cleanLists.add(list); // Se � nova, e temos indexa��o, inclu�mos a lista para limpeza depois.
                        // Se temos um index, temos de repeita-lo e ir populando a lista corretamente. Como podemos receber a lista fora de ordem temos de verificar o tamanho da lista antes de inserir o objeto. Caso a lista ainda seja menor do que a posi��o a ser ocupada, inclu�mos alguns "Dummy" Objects para crescer a lista e j� deixar o objeto na posi��o correta
                        while (list.size() <= sortIndex + 3)
                          list.add(map); // incluimos um objeto que j� existe para evitar instanciar novos objetos na mem�ria. E Adicionar NULL n�o funciona na Linked List
                        // Inclu�mos inclusive um objeto no lugar onde nosso objeto ser� colocado propositalmente, assim n�o precisamos ter duas l�gicas abaixo
                        list.add(sortIndex, vo); // Ao incluir o objeto no �ndex determinado, empurramos todos os outros para frente, por isso temos de remover o pr�ximo objeto (que antes ocupava o lugar deste)
                        list.remove(sortIndex + 1);
                      }
                      RUReflex.setPropertyValue(join, relativePath, list, false);
                    }
                  }
                } else if (Map.class.isAssignableFrom(rt)) {
                  if (iterationControl == 1) {
                    Map hash = (Map) RUReflex.getPropertyValue(join, relativePath);
                    if (hash == null) {
                      hash = new LinkedHashMap<>();
                    }
                    // Recupera o atributo do objeto que � usado como chave da hash
                    final String keyMapAttributeName = join.getClass().getDeclaredField(relativePath).getAnnotation(RFWMetaRelationshipField.class).keyMap();
                    final Object key = RUReflex.getPropertyValue(vo, keyMapAttributeName);
                    if (!hash.containsKey(key)) {
                      hash.put(key, vo); // S� adiciona se ainda n�o tiver este objeto
                      RUReflex.setPropertyValue(join, relativePath, hash, false);
                    }
                  }
                } else {
                  throw new RFWCriticalException("O RFWDAO n�o sabe montar mapeamento do tipo '${0}', presente no '${1}'.", new String[] { rt.getCanonicalName(), join.getClass().getCanonicalName() });
                }
              } else {
                // Se o objeto da tabela n�o foi montado, verifica se a chave consta na Hash. Isso n�o h� objeto para associar, mas que o objeto foi procurado. Nesse caso n�o temos objeto para adicionar, mas devemos criar a Lista/Hash Vazia se ela ainda n�o existir
                if (aliasCache.containsKey(mTable.alias)) {
                  RFWVO join = aliasCache.get(mTable.joinAlias);
                  if (join == null) {
                    // Join � nulo quando temos entre este objeto e o objeto de joinAlias uma tabela de N:N. Neste caso temos de pular esse objeto
                    final DAOMapTable tmp = map.getMapTableByAlias(mTable.joinAlias);
                    // Se for mesmo a tabela de Join, ela tem o caminho come�ando com ".". Se n�o for uma tabela de join n�o utilizamos o objeto pois pode ser s� um caso de 2 Joins de objetos diferentes e estamos pulando 1
                    if (tmp.path.startsWith(".")) join = aliasCache.get(tmp.joinAlias);
                  }
                  if (join != null) {
                    // Se joinAlias continuar nulo, � pq mesmo o objeto pai n�o foi montado. Isso pode acontecer am casos de m�ltiplos LEFT JOIN e o relacionamento anterior tamb�m retornou nulo. Como ele � nulo n�o precisamos criar a lista vazia
                    final String relativePath = RUReflex.getLastPath(mTable.path); // pega o caminho em rala��o ao objeto atual, n�o desde a raiz
                    final Class<?> rt = RUReflex.getPropertyTypeByType(join.getClass(), relativePath);
                    if (RFWVO.class.isAssignableFrom(rt)) {
                      // N�o faz nada, s� n�o deixa cair no else
                    } else if (List.class.isAssignableFrom(rt)) {
                      List list = (List) RUReflex.getPropertyValue(join, relativePath);
                      if (list == null) {
                        list = new ArrayList<>();
                        RUReflex.setPropertyValue(join, relativePath, list, false);
                      }
                    } else if (Map.class.isAssignableFrom(rt)) {
                      Map hash = (Map) RUReflex.getPropertyValue(join, relativePath);
                      if (hash == null) {
                        hash = new LinkedHashMap<>();
                        RUReflex.setPropertyValue(join, relativePath, hash, false);
                      }
                    } else {
                      throw new RFWCriticalException("O RFWDAO n�o sabe montar mapeamento do tipo '${0}', presente no '${1}'.", new String[] { rt.getCanonicalName(), join.getClass().getCanonicalName() });
                    }
                  }
                }
              }
            }
          }
        }
      }

      // Limpamos as listas marcadas para limpeza
      for (List<?> list : cleanLists) {
        int size = -1;
        while (size != list.size()) {
          size = list.size();
          list.remove(map);
        }
      }

      return vos;
    } catch (RFWException e) {
      throw e;
    } catch (Exception e) {
      throw new RFWCriticalException("Falha ao montar objeto com os dados retornados do banco de dados.", e);
    }
  }

  private Boolean getRSBoolean(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    Boolean l = null;
    switch (dialect) {
      case MySQL:
        try {
          l = rs.getBoolean(tableAlias + "." + column);
          if (rs.wasNull()) l = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              l = rs.getBoolean(i);
              if (rs.wasNull()) l = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return l;
  }

  private BigDecimal getRSBigDecimal(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    BigDecimal l = null;
    switch (dialect) {
      case MySQL:
        try {
          l = rs.getBigDecimal(tableAlias + "." + column);
          if (rs.wasNull()) l = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              l = rs.getBigDecimal(i);
              if (rs.wasNull()) l = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return l;
  }

  private Blob getRSBlob(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    Blob l = null;
    switch (dialect) {
      case MySQL:
        try {
          l = rs.getBlob(tableAlias + "." + column);
          if (rs.wasNull()) l = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              l = rs.getBlob(i);
              if (rs.wasNull()) l = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return l;
  }

  private Time getRSTime(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    Time l = null;
    switch (dialect) {
      case MySQL:
        try {
          l = rs.getTime(tableAlias + "." + column);
          if (rs.wasNull()) l = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              l = rs.getTime(i);
              if (rs.wasNull()) l = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return l;
  }

  private Timestamp getRSTimestamp(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    Timestamp l = null;
    switch (dialect) {
      case MySQL:
        try {
          l = rs.getTimestamp(tableAlias + "." + column);
          if (rs.wasNull()) l = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              l = rs.getTimestamp(i);
              if (rs.wasNull()) l = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return l;
  }

  private Long getRSLong(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    Long l = null;
    switch (dialect) {
      case MySQL:
        try {
          l = rs.getLong(tableAlias + "." + column);
          if (rs.wasNull()) l = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              l = rs.getLong(i);
              if (rs.wasNull()) l = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return l;
  }

  private Object getRSObject(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    Object l = null;
    switch (dialect) {
      case MySQL:
        try {
          l = rs.getObject(tableAlias + "." + column);
          if (rs.wasNull()) l = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              l = rs.getObject(i);
              if (rs.wasNull()) l = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return l;
  }

  private Integer getRSInteger(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    Integer l = null;
    switch (dialect) {
      case MySQL:
        try {
          l = rs.getInt(tableAlias + "." + column);
          if (rs.wasNull()) l = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              l = rs.getInt(i);
              if (rs.wasNull()) l = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return l;
  }

  private Float getRSFloat(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    Float l = null;
    switch (dialect) {
      case MySQL:
        try {
          l = rs.getFloat(tableAlias + "." + column);
          if (rs.wasNull()) l = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              l = rs.getFloat(i);
              if (rs.wasNull()) l = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return l;
  }

  private String getRSString(ResultSet rs, String schema, String tableName, String tableAlias, String column, SQLDialect dialect) throws RFWException {
    String s = null;
    switch (dialect) {
      case MySQL:
        try {
          s = rs.getString(tableAlias + "." + column);
          if (rs.wasNull()) s = null;
          // Essa flag � passada para true quando n�o tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, s� retornou nulo. Isso quer dizer buscamos pelo objeto mas n�o existe a associa��o.
          // Neste caso temos de inicializar as listas do objeto, mesmo que v� vazia, para indicar que procuramos pelas associa��es mesmo qu n�o exista nenhuma. J� que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB n�o d� suporte � recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
          String col = (schema + "." + tableName + "." + column).toUpperCase();
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            if (col.equals(md.getSchemaName(i) + "." + md.getTableName(i) + "." + md.getColumnName(i))) {
              s = rs.getString(i);
              if (rs.wasNull()) s = null;
              break;
            }
          }
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
    }
    return s;
  }

  private RFWVO fullFillCompositoinTreeObject(DAOMap map, DAOMapTable startTable, RFWVO vo, HashMap<String, RFWVO> objCache) throws RFWException {
    // Se n�o for a tabela raiz, temos de criar um subMap para conseguir prosseguir, se for, j� estamos com ele pronto (provavelmente pq j� estamos seguindo a �rvore desse objeto
    if (!"".equals(startTable.path)) map = map.createSubMap(startTable);
    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectCompositionTreeStatement(conn, map, startTable, vo.getId(), null, null, null, null, null, dialect); ResultSet rs = stmt.executeQuery()) {
      // Removemos o objeto atual da Cache, ou ele n�o ser� remontado conforme os novos dados
      objCache.remove(vo.getClass().getCanonicalName() + "." + vo.getId());
      final List<RFWVO> list = mountVO(rs, map, objCache);
      if (list.size() > 1) {
        throw new RFWCriticalException("O RFWDAO montou mais de um objeto de CompositionTree para o mesmo ID! Alguma falha de modelo da tabelas ou configura��o de chaves de banco.");
      } else if (list.size() == 1) {
        return list.get(0);
      }
    } catch (RFWException e) {
      throw e;
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a opera��o no banco de dados.", e);
    }
    throw new RFWCriticalException("O RFWDAO falhou em montar o CompositionTree para um objeto que acabara de ser recuperado do banco.");
  }

  /**
   * Este m�todo recupera o conte�do do ResultSet, e de acordo com o tipo do atributo no VO, faz a convers�o e passa o valor.
   *
   * @param mField Descritor do Mapeamento do Campo
   * @param vo VO onde a informa��o ser� escrita
   * @param rs ResultSet com o conte�do do banco de dados.
   * @throws RFWException
   */
  private void writeToVO(DAOMapField mField, VO vo, ResultSet rs) throws RFWException {
    try {
      final DAOMapTable mTable = mField.table;
      final Field decField = RUReflex.getDeclaredFieldRecursively(mTable.type, mField.field);

      // Buscamos se o atributo tem algum converter definido
      final RFWDAOConverter convAnn = decField.getAnnotation(RFWDAOConverter.class);
      if (convAnn != null) {
        final Object ni = createNewInstance(convAnn.converterClass());
        if (!(ni instanceof RFWDAOConverterInterface)) throw new RFWCriticalException("A classe '${0}' definida no atributo '${1}' da classe '${2}' n�o � um RFWDAOConverterInterface v�lido!", new String[] { convAnn.converterClass().getCanonicalName(), mField.field, vo.getClass().getCanonicalName() });
        Object obj = getRSObject(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect);
        // final Object s = ((RFWDAOConverterInterface) ni).toVO(rs.getObject(mTable.alias + "." + mField.column));
        final Object s = ((RFWDAOConverterInterface) ni).toVO(obj);
        RUReflex.setPropertyValue(vo, mField.field, s, false);
      } else {
        final Class<?> dataType = decField.getType();

        if (Long.class.isAssignableFrom(dataType)) {
          Long l = getRSLong(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getLong(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) RUReflex.setPropertyValue(vo, mField.field, l, false);
        } else if (String.class.isAssignableFrom(dataType)) {
          String s = getRSString(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getString(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) {
            // Nos casos de String, verificamos se temos a anotation de RFWMetaEncrypt
            final RFWMetaEncrypt encAnn = decField.getAnnotation(RFWMetaEncrypt.class);
            if (encAnn != null) {
              s = RUEncrypter.decryptDES(s, encAnn.key());
            }
            RUReflex.setPropertyValue(vo, mField.field, s, false);
          }
        } else if (Date.class.isAssignableFrom(dataType)) {
          if (RFW.isDevelopmentEnvironment() && !RFW.isDevPropertyTrue("rfw.orm.dao.disableLocalDateTimeRecomendation")) {
            // Se estiver no desenvolvimento imprime a exception com a mensagem de recomenda��o para que tenha o Stack da chamada completa, mas deixa o c�digo seguir normalmente
            new RFWWarningException("O RFW n�o recomenda utilizar o 'java.util.Date'. Verifique a implementa��o e substitua adequadamente por LocalDate, LocalTime ou LocalDateTime.").printStackTrace();
          }
          Timestamp timestamp = rs.getTimestamp(mField.table.alias + "." + mField.column);
          if (!rs.wasNull()) RUReflex.setPropertyValue(vo, mField.field, new Date(timestamp.getTime()), false);
        } else if (LocalDate.class.isAssignableFrom(dataType)) {
          Object obj = getRSObject(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getObject(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) {
            if (obj instanceof Timestamp) {
              RUReflex.setPropertyValue(vo, mField.field, ((Timestamp) obj).toLocalDateTime().toLocalDate(), false);
            } else if (obj instanceof java.sql.Date) {
              RUReflex.setPropertyValue(vo, mField.field, ((java.sql.Date) obj).toLocalDate(), false);
            } else {
              throw new RFWCriticalException("N�o foi poss�vel identificar o objeto '" + obj.getClass().getCanonicalName() + "' recebido para o atributo '" + mField.field + "' do VO: '" + vo.getClass().getCanonicalName() + "'. Tabela: '" + mTable.table + "." + mField.column + "'.");
            }
          }
        } else if (LocalTime.class.isAssignableFrom(dataType)) {
          Time t = getRSTime(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getTime(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) RUReflex.setPropertyValue(vo, mField.field, t.toLocalTime(), false);
        } else if (LocalDateTime.class.isAssignableFrom(dataType)) {
          Timestamp t = getRSTimestamp(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getTimestamp(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) RUReflex.setPropertyValue(vo, mField.field, t.toLocalDateTime(), false);
        } else if (Integer.class.isAssignableFrom(dataType)) {
          Integer i = getRSInteger(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getInt(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) RUReflex.setPropertyValue(vo, mField.field, i, false);
        } else if (Float.class.isAssignableFrom(dataType)) {
          Float i = getRSFloat(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getInt(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) RUReflex.setPropertyValue(vo, mField.field, i, false);
        } else if (Boolean.class.isAssignableFrom(dataType)) {
          Boolean b = getRSBoolean(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getBoolean(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) RUReflex.setPropertyValue(vo, mField.field, b, false);
        } else if (BigDecimal.class.isAssignableFrom(dataType)) {
          BigDecimal b = getRSBigDecimal(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getBigDecimal(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) RUReflex.setPropertyValue(vo, mField.field, b, false);
        } else if (Enum.class.isAssignableFrom(dataType)) {
          String b = getRSString(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getString(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) {
            try {
              @SuppressWarnings({ "rawtypes", "unchecked" })
              final Enum e = Enum.valueOf((Class<Enum>) dataType, b);
              RUReflex.setPropertyValue(vo, mField.field, e, false);
            } catch (IllegalArgumentException e) {
              throw new RFWCriticalException("RFW_ERR_000013", new String[] { b, dataType.getCanonicalName(), mField.field, vo.getClass().getCanonicalName() });
            }
          }
        } else if (byte[].class.isAssignableFrom(dataType)) {
          Blob blob = getRSBlob(rs, mTable.schema, mTable.table, mTable.alias, mField.column, dialect); // rs.getBlob(mTable.alias + "." + mField.column);
          if (!rs.wasNull()) {
            int blobLength = (int) blob.length();
            byte[] b = blob.getBytes(1, blobLength);
            blob.free();
            if (!rs.wasNull()) RUReflex.setPropertyValue(vo, mField.field, b, false);
          }
        } else if (RFWVO.class.isAssignableFrom(dataType)) {
          // N�o fazemos nada. Isso pq o caso em que esse objeto aparece � quando temos uma coluna de FK na tabela do objeto. Para inserir s� o ID n�o vamos criar o objeto para dar prefer�ncia em mandar sempre um objeto mais leve. Caso o objeto tenha sido solicitado explicitamente, ele ser� montado durante leitura da sua propria tabela e colocado aqui.
          // Assim inclusive garantimos que n�o vamos sobrepor um objeto completo por um s� com o ID
        } else if (List.class.isAssignableFrom(dataType)) {
          // N�o fazemos nada. Isso pq o caso em que esse objeto aparece � quando temos uma coluna de FK na tabela do objeto. Para inserir s� o ID n�o vamos criar o objeto para dar prefer�ncia em mandar sempre um objeto mais leve. Caso o objeto tenha sido solicitado explicitamente, ele ser� montado durante leitura da sua propria tabela e colocado aqui.
          // Assim inclusive garantimos que n�o vamos sobrepor um objeto completo por um s� com o ID
        } else {
          throw new RFWCriticalException("O RFWDAO n�o escrever no VO dados do tipo '${0}'.", new String[] { dataType.getCanonicalName() });
        }
      }
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao montar objeto com os dados retornados do banco de dados.", new String[] { mField.path + "." + mField.field }, e);
    }

  }

  private DAOMap createDAOMap(Class<VO> type, String[] attributes) throws RFWException {
    final DAOMap map = new DAOMap();

    // Primeiro passo, carregar os mapeamentos da entidade raiz
    loadEntityMap(type, map, "");

    // Iteramos todos os atributos para mapea-los tamb�m
    if (attributes != null) for (String attribute : attributes) {
      loadEntityMap(type, map, RUReflex.getParentPath(attribute)); // Removemos o �ltimo bloco de attribute pq � um atributo, e queremos passar para o m�todo somente o "caminho" at� a entidade.
      loadEntityCollectionMap(type, map, attribute);
    }

    return map;
  }

  /**
   * Carrega o mapeamento de uma entidade (RFWVO) na estrutura de mapeamento do SQL.
   *
   * @param type Tipo da Entidade a ser carregada (Classe)
   * @param map Objeto de mapeamento onde os novos mapeamentos devem ser colocados.
   * @param path Caminho do atributo com o mapeamento para esta entidade. Passe "" para carregar como objeto raiz. NUNCA PASSE NULL, n�o testamos null para diminuir o if.
   * @throws RFWException
   */
  @SuppressWarnings("unchecked")
  private void loadEntityMap(Class<? extends RFWVO> root, DAOMap map, String path) throws RFWException {
    if (map.getMapTableByPath(path) == null) { // se j� encontramos n�o precisamos processar o bloco abaixo, pois n�o s� o mapeamento da tabela j� foi criado, qualquer qualquer mapeamento de tabela N:N e seus campos
      // Vari�veis com o prefixo "entity" referen-se � entidade sendo mapeada
      Class<? extends RFWVO> entityType = null;
      RFWDAOAnnotation entityDAOAnn = null;
      String entityTable = null;
      String entitySchema = null;
      String entityJoin = null;
      String entityColumn = null;
      String entityJoinColumn = null;
      DAOMapTable mapTable = null;

      if (path.equals("")) { // Deixado nessa ordem, ao inv�s do "".equals(path) para justamente dar nullpointer caso algu�m passe null. Em caso de null o if retornria true e estragaria a l�gica de qualquer forma. Como null n�o � esperado, conforme javadoc, � melhor que d� nullpointer logo aqui para que o real problema seja encontrado (que � de onde vem o null)
        entityType = getEntity(root); // Se estamos no objeto raiz, a entidade � exatamente o objeto raiz
        entityDAOAnn = entityType.getAnnotation(RFWDAOAnnotation.class);
        entityTable = getTable(entityType, entityDAOAnn);
        entitySchema = getSchema(entityType, entityDAOAnn);

        PreProcess.requiredNonNull(entitySchema, "O RFWDAO n�o conseguiu determinar o schema a ser utilizada com a entidade: '${0}'.", new String[] { entityType.getCanonicalName() });
        PreProcess.requiredNonNull(entityTable, "O RFWDAO n�o conseguiu determinar a tabela a ser utilizada com a entidade: '${0}'.", new String[] { entityType.getCanonicalName() });

        mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, entityColumn, entityJoin, entityJoinColumn);
      } else {
        String parentPath = RUReflex.getParentPath(path);
        if (parentPath != null) {
          // Se temos um caminho e obtivemos um caminho do pai, procuramos se essa tabela j� foi mapeada
          DAOMapTable parentMapTable = map.getMapTableByPath(parentPath);
          if (parentMapTable == null) {
            // Se n�o encontramos, chamamos o m�todo recursivamente para primeiro cadastrar o pai
            loadEntityMap(root, map, parentPath);
            // Recuperamos o parent recem criado na recurs�o
            parentMapTable = map.getMapTableByPath(parentPath);
          }

          // Se temos o pai, temos de verificar o relacionamento que entre o objeto pai e este e suas defini��es. Dependendo do tipo de relacionamento, posicionamento da FK e etc., o mapeamento ser� diferente
          Field parentField = null;
          try {
            parentField = parentMapTable.type.getDeclaredField(RUReflex.getCleanPath(RUReflex.getLastPath(path)));
          } catch (Exception e) {
            throw new RFWCriticalException("", new String[] { RUReflex.getCleanPath(RUReflex.getLastPath(path)), parentMapTable.type.getCanonicalName() }, e);
          }
          final RFWMetaRelationshipField parentRelAnn = parentField.getAnnotation(RFWMetaRelationshipField.class);

          // Para definir a entidade destino primeiro tentamos retirar do atributo da anota��o da classe pai, se n�o tiver tentamos verificar o tipo do atributo. A ordem � usada assim pr conta de litas, hashs e interfaces que impedem a detec��o automaticamente
          if (!parentRelAnn.targetRelationship().equals(RFWVO.class)) {
            entityType = parentRelAnn.targetRelationship();
          } else if (RFWVO.class.isAssignableFrom(parentField.getType())) {
            entityType = (Class<? extends RFWVO>) parentField.getType();
          } else {
            throw new RFWCriticalException("N�o foi poss�vel detectar a classe de relacionamento do atributo '${0}'. Verifique se � um RFWVO ou se a classe est� definida corretamente no 'targetRelatioship'.", new String[] { parentMapTable.type.getCanonicalName() + "." + parentField.getName() });
          }

          entityType = getEntity(entityType);
          entityDAOAnn = entityType.getAnnotation(RFWDAOAnnotation.class);
          entitySchema = getSchema(entityType, entityDAOAnn);
          entityTable = getTable(entityType, entityDAOAnn);

          PreProcess.requiredNonNull(entitySchema, "O RFWDAO n�o conseguiu determinar o schema a ser utilizada com a entidade: '${0}'.", new String[] { entityType.getCanonicalName() });
          PreProcess.requiredNonNull(entityTable, "O RFWDAO n�o conseguiu determinar a tabela a ser utilizada com a entidade: '${0}'.", new String[] { entityType.getCanonicalName() });

          switch (parentRelAnn.relationship()) {
            case MANY_TO_MANY:
              // Se o relacionamento entre um objeto e outro � N:N temos de criar um mapeamento de tabela de "joinAlias", se ainda n�o existir
              DAOMapTable joinMapTable = map.getMapTableByPath("." + path); // Tabelas de Join tem o mesmo caminho da tabela da entidade relacionada, precedidas de um '.'
              if (joinMapTable == null) joinMapTable = map.createMapTable(null, "." + path, parentMapTable.schema, getMetaRelationJoinTable(parentField, parentRelAnn), getMetaRelationColumn(parentField, parentRelAnn), parentMapTable.alias, "id");
              // Usamos as informa��es do mapeamento de joinAlias criado para criar o mapeamento desta entidade agora
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, "id", joinMapTable.alias, getMetaRelationColumnMapped(parentField, parentRelAnn));
              break;
            case PARENT_ASSOCIATION:
              if ("".equals(getMetaRelationColumn(parentField, parentRelAnn))) throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' est� marcado como PARENT_ASSOCIATION, mas n�o tem o atributo 'column' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, "id", parentMapTable.alias, getMetaRelationColumn(parentField, parentRelAnn));
              break;
            case WEAK_ASSOCIATION:
              // No mapeamento, este tipo de relacionamento se comporta igualzinho o ASSOCIATION, por isso deixamos seguir para o ASSOCIATION e realizar o mesmo tipo de mapeamento/regras.
            case ASSOCIATION:
              if (!"".equals(getMetaRelationColumn(parentField, parentRelAnn))) {
                // Se a coluna mapeada est� na tabela pai
                mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, "id", parentMapTable.alias, getMetaRelationColumn(parentField, parentRelAnn));
              } else if (!"".equals(getMetaRelationColumnMapped(parentField, parentRelAnn))) {
                // Se a coluna mapeada est� na nossa tabela
                mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, getMetaRelationColumnMapped(parentField, parentRelAnn), parentMapTable.alias, "id");
              } else {
                throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' est� marcado como ASSOCIATION, mas n�o tem o atributo 'column' nem 'columMapper' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              }
              break;
            case COMPOSITION:
              // Se � composi��o, criamos o mapeamento considerando que a coluna de joinAlias est� na tabela que estamos mapeando agora
              if ("".equals(getMetaRelationColumnMapped(parentField, parentRelAnn))) throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' est� marcado como COMPOSITION, mas n�o tem o atributo 'columnMapped' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, getMetaRelationColumnMapped(parentField, parentRelAnn), parentMapTable.alias, "id");
              break;
            case COMPOSITION_TREE:
              // Se � composi��o de hierarquia, criamos o mapeamento s� do primeiro objeto mas n�o vamos dar sequ�ncia recursivamente. A sequ�ncia recursiva dever� ser tratada dinamicamente no objeto posteriormente
              if ("".equals(getMetaRelationColumnMapped(parentField, parentRelAnn))) throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' est� marcado como COMPOSITION_TREE, mas n�o tem o atributo 'columnMapped' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, getMetaRelationColumnMapped(parentField, parentRelAnn), parentMapTable.alias, "id");
              break;
            case INNER_ASSOCIATION:
              // Uma associa��o interna � similar a uma PARENT ASSOCIATION, mesmo que o RFWDAO v� reutilizar o objeto caso ele j� exista, em casos de consultas espec�ficas precisamos fazer o Join da tabela de qualquer forma.
              if ("".equals(getMetaRelationColumn(parentField, parentRelAnn))) throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' est� marcado como INNER_ASSOCIATION, mas n�o tem o atributo 'column' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, "id", parentMapTable.alias, getMetaRelationColumn(parentField, parentRelAnn));
              break;
          }
        }
      }

      // mapeamos o atributo "id" que n�o � enxergado pq est� na classe pai RFWVO.
      map.createMapField(path, "id", mapTable, "id"); // Este atributo segue o padr�o que no banco de ser sempre "id" e no objeto herda sempre o atributo "id" da classe pai.
      // RFWDAO.dumpDAOMap(map)
      // Tendo o mapeamento da tabela feito, iteramos a classe para mapear todos os seus fields que tenham alguma anota��o RFWMeta definida. Qualquer attributo sem uma annotation RFWMeta � ignorado.
      for (Field field : RUReflex.getDeclaredFieldsRecursively(entityType)) {
        final Annotation metaAnn = RUReflex.getRFWMetaAnnotation(field);
        if (metaAnn != null) {

          // Se n�o tivermos um nome de coluna definido, utilizaremos o nome do pr�prio atributo da classe
          String fieldColumn = field.getName();
          try {
            // Tentamos recuperar por reflex�o o field "column" da annotation j� que n�o sabemos qual annotation �. Se falhar n�o tem problema, deixa o c�digo seguir e tentar usar o nome da coluna como sendo do atributo da classe.
            String t = (String) metaAnn.annotationType().getMethod("column").invoke(metaAnn);
            if (!"".equals(t)) fieldColumn = t; // S� passamos se n�o estiver "", j� que "" � o valor padr�o do atributo column na annotation.
          } catch (Throwable e) {
            throw new RFWCriticalException("N�o encontramos o atributo 'column' na RFWMetaAnnotation '${0}'. Este atributo � obrigat�rio em todas as RFWMetaAnnotations.", new String[] { metaAnn.annotationType().getCanonicalName() });
          }

          if (metaAnn instanceof RFWMetaRelationshipField) {
            RFWMetaRelationshipField relAnn = (RFWMetaRelationshipField) metaAnn;
            if (relAnn.relationship() != RelationshipTypes.MANY_TO_MANY && !"".equals(getMetaRelationColumn(field, relAnn))) {
              // Se a coluna com a FK fica na tabela deste objeto (ou seja o column foi definido e n�o � um relacionamento N:N)
              map.createMapField(path, field.getName(), mapTable, getMetaRelationColumn(field, relAnn));
            } else {
              // Relacionamentos que n�o tenham a FK dentro da tabela do objeto em quest�o, n�o precisam ser mapeadas. Isso pq ou n�o ser�o recuperados de qualquer forma, ou ser�o mapeados depois no mapa da entidade associada.
            }
          } else if (metaAnn instanceof RFWMetaCollectionField) {
            // Se for um collection n�o cadastramos nada neste momento, os atirbutos de collection precisam ser solicitados explicitamente.
            // Ao recebermos o atributo do collection ele � tratado posteriormente no m�todo loadEntityCollectionMap
          } else {
            // Se n�o for um Relationship, deixamos simplesmente cadastramos o mapeamento comum
            map.createMapField(path, field.getName(), mapTable, fieldColumn);
          }
        }
      }
    }
  }

  private void loadEntityCollectionMap(Class<? extends RFWVO> root, DAOMap map, String attribute) throws RFWException {
    if (!"id".equals(attribute)) {
      if (attribute.endsWith("@") && attribute.length() > 0) attribute = attribute.substring(0, attribute.length() - 1);

      // Verifica se ser� substitu�do
      Class<? extends RFWVO> entityType = getEntity(root);

      Annotation ann = RUReflex.getRFWMetaAnnotation(entityType, attribute);
      if (ann instanceof RFWMetaCollectionField) {
        RFWMetaCollectionField colAnn = (RFWMetaCollectionField) ann;
        String parentPath = RUReflex.getParentPath(attribute); // Recuperamos o caminho pai para obter o mapeamento do pai. J� devemos ter todos pois o m�todo loadEntityMap deve ser sempre chamado antes deste m�todo

        String[] paths = attribute.split("\\.");
        String fieldName = paths[paths.length - 1];

        // Validamos se j� n�o associamos essa tabela (isso pode acontecer se o usu�rio solicitar mais de uma vez o mesmo atributo de collection... estupido...)
        if (map.getMapTableByPath("@" + RUReflex.addPath(parentPath, fieldName)) == null) {
          DAOMapTable daoMapTable = map.getMapTableByPath(parentPath);

          // para indicar que o caminho � uma Collection, o path recebe um '@' como prefixo do path
          final DAOMapTable colMapTable = map.createMapTable(daoMapTable.type, "@" + RUReflex.addPath(parentPath, fieldName), daoMapTable.schema, colAnn.table(), colAnn.fkColumn(), daoMapTable.alias, "id");
          // Mapeia o campo e as colunas de key e Sort caso existam
          map.createMapField(parentPath, fieldName + "@", colMapTable, colAnn.column());
          map.createMapField(parentPath, fieldName + "@fk", colMapTable, colAnn.fkColumn());
          if (!"".equals(colAnn.keyColumn())) map.createMapField(parentPath, fieldName + "@keyColumn", colMapTable, colAnn.keyColumn());
          if (!"".equals(colAnn.sortColumn())) map.createMapField(parentPath, fieldName + "@sortColumn", colMapTable, colAnn.sortColumn());
        }
      }
    }
  }

  /**
   * Retorna a entitidade que deve ser utilizada para mapear o DAOMap.<br>
   * Este m�todo permite que o {@link DAOResolver} substitua um objeto por outro a ser mapeado em seu lugar.
   *
   * @param entityType Entidate a descobrir o Schema
   * @param entityDAOAnn
   * @return
   * @throws RFWException
   */
  private Class<? extends RFWVO> getEntity(Class<? extends RFWVO> entityType) throws RFWException {
    Class<? extends RFWVO> newEntity = null;
    // Solicita no Resolver
    if (this.resolver != null) {
      newEntity = this.resolver.getEntityType(entityType);
    }
    if (newEntity == null) {
      throw new RFWCriticalException("O DAOResolver retornou nulo ao resolver a entidade: '" + entityType.getCanonicalName() + "'.");
    }
    return newEntity;
  }

  /**
   * Retorna o schema/catalog conforme defini��o da annotation da entidade.
   *
   * @param entityType Entidate a descobrir o Schema
   * @param entityDAOAnn
   * @return
   * @throws RFWException
   */
  private String getSchema(Class<? extends RFWVO> entityType, final RFWDAOAnnotation entityDAOAnn) throws RFWException {
    String schema = null;
    // Solicita no Resolver
    if (this.resolver != null) {
      schema = this.resolver.getSchema(entityType, entityDAOAnn);
    }
    // Verifica se foi pasado na constru��o do RFWDAO
    if (schema == null) {
      schema = this.schema;
    }
    // Verifica se temos na entidade
    if (schema == null) {
      schema = entityDAOAnn.schema();
    }
    if (schema == null) {
      throw new RFWCriticalException("N�o h� um schema definido para a entidade '" + entityType.getCanonicalName() + "'.");
    }
    return schema;
  }

  private Object createNewInstance(Class<?> objClass) throws RFWException {
    Object newInstance = null;
    if (this.resolver != null) {
      newInstance = this.resolver.createInstance(objClass);
    }
    if (newInstance == null) {
      try {
        newInstance = objClass.newInstance();
      } catch (Throwable e) {
        throw new RFWCriticalException("RFW_000021", new String[] { objClass.getCanonicalName() }, e);
      }
    }
    return newInstance;
  }

  /**
   * Retorna a tabela conforme defini��o da annotation da entidade.
   *
   * @param entityType Entidate a descobrir o Schema
   * @param entityDAOAnn
   * @return
   * @throws RFWException
   */
  private String getTable(Class<? extends RFWVO> entityType, final RFWDAOAnnotation entityDAOAnn) throws RFWException {
    String table = null;
    // Solicita no Resolver
    if (this.resolver != null) {
      table = this.resolver.getTable(entityType, entityDAOAnn);
    }
    // Verifica se temos na entidade
    if (table == null) {
      table = entityDAOAnn.table();
    }
    if (table == null) {
      throw new RFWCriticalException("N�o h� uma tabela definida para a entidade '" + entityType.getCanonicalName() + "'.");
    }
    return table;
  }

  /**
   * M�todo para DEBUG, retorna uma String (e imprime no console) o conte�do do Map, tanto mapeamento das tabelas quanto dos campos.
   *
   * @param map Map a ser impresso.
   * @return String contendo o dump do {@link DAOMap}
   * @throws RFWException
   */
  public static String dumpDAOMap(DAOMap map) throws RFWException {
    // br.eng.rodrigogml.rfw.kernel.utils.RUFile.writeFileContent("c:\\t\\dumpDAOMap.txt", RFWDAO.dumpDAOMap(map));
    // RFWDAO.dumpDAOMap(daoMap)
    StringBuilder buff = new StringBuilder();
    buff.append(System.lineSeparator()).append(System.lineSeparator());
    buff.append("MAPEAMENTO DAS ENTIDADES").append(System.lineSeparator());
    buff.append(RUString.completeUntilLengthRight("-", "", 365)).append(System.lineSeparator());
    buff.append(RUString.completeUntilLengthRight(" ", "Path", 100));
    buff.append(RUString.completeUntilLengthRight(" ", "Schema.Table", 60));
    buff.append(RUString.completeUntilLengthRight(" ", "Alias", 10));
    buff.append(RUString.completeUntilLengthRight(" ", "Column", 30));
    buff.append(RUString.completeUntilLengthRight(" ", "Join", 10));
    buff.append(RUString.completeUntilLengthRight(" ", "JoinColumn", 30));
    buff.append(RUString.completeUntilLengthRight(" ", "Entity", 0));
    buff.append(System.lineSeparator());
    buff.append(RUString.completeUntilLengthRight("-", "", 365)).append(System.lineSeparator());

    for (DAOMapTable mTable : map.getMapTable()) {
      buff.append(RUString.completeUntilLengthRight(" ", mTable.path, 100));
      buff.append(RUString.completeUntilLengthRight(" ", mTable.schema + "." + mTable.table, 60));
      buff.append(RUString.completeUntilLengthRight(" ", mTable.alias, 10));
      buff.append(RUString.completeUntilLengthRight(" ", mTable.column, 30));
      buff.append(RUString.completeUntilLengthRight(" ", mTable.joinAlias, 10));
      buff.append(RUString.completeUntilLengthRight(" ", mTable.joinColumn, 30));
      buff.append(RUString.completeUntilLengthRight(" ", (mTable.type != null ? mTable.type.getCanonicalName() : "null"), 0));
      buff.append(System.lineSeparator());
    }

    buff.append(RUString.completeUntilLengthRight("-", "", 365)).append(System.lineSeparator());

    // Escreve Tabela de Fields
    buff.append(System.lineSeparator());
    buff.append("MAPEAMENTO DOS ATRIBUTOS").append(System.lineSeparator());
    buff.append(RUString.completeUntilLengthRight("-", "", 365)).append(System.lineSeparator());
    buff.append(RUString.completeUntilLengthRight(" ", "Path", 100));
    buff.append(RUString.completeUntilLengthRight(" ", "Field", 50));
    buff.append(RUString.completeUntilLengthRight(" ", "Schema.Table", 120));
    buff.append(RUString.completeUntilLengthRight(" ", "Column", 0));
    buff.append(System.lineSeparator());
    buff.append(RUString.completeUntilLengthRight("-", "", 365)).append(System.lineSeparator());

    for (DAOMapField mField : map.getMapField()) {
      buff.append(RUString.completeUntilLengthRight(" ", mField.path, 100));
      buff.append(RUString.completeUntilLengthRight(" ", mField.field, 50));
      buff.append(RUString.completeUntilLengthRight(" ", "(" + mField.table.alias + ") " + mField.table.schema + "." + mField.table.table, 120));
      buff.append(RUString.completeUntilLengthRight(" ", mField.column, 0));
      buff.append(System.lineSeparator());
    }

    buff.append(RUString.completeUntilLengthRight("-", "", 365)).append(System.lineSeparator());

    // imprime e retorna
    final String s = buff.toString();
    if (RFW.isDevelopmentEnvironment()) RUFile.writeFileContent("c:\\t\\dumpDAOMap.txt", s);
    return s;
  }
}
