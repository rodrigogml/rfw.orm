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
 * @author Rodrigo Leitão
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
     * Indica se ao criar o statement de insert não escrever ID atribuindo valor igual a null.<br>
     * No Derby, quando temos uma coluna de IDs gerada automaticamente ela não pode aparecer no statement.
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
     * # indica se ao criar o statement de insert não escrever ID atribuindo valor igual a null.<br>
     * No Derby, quando temos uma coluna de IDs gerada automaticamente ela não pode aparecer no statement.
     *
     * @return the indica se ao criar o statement de insert não escrever ID atribuindo valor igual a null
     */
    public boolean getSkipInsertIDColumn() {
      return skipInsertIDColumn;
    }

  }

  /**
   * Objeto utilizado para registrar pendências de inserção de objetos cruzados.<br>
   * Por exemplo, o Framework precisa inserir um objeto que tem uma associação com outro que ainda não foi inserido (ainda não tem um ID).<br>
   * Nestes caso a lógica de persistência cria um objeto desses registrando que o objeto foi persistido, mas que é necessário atualizar a associação quando o outro objeto estiver persistido também.<br>
   * Esta estrutura é utilizada principalmente em casos do {@link RelationshipTypes#INNER_ASSOCIATION}.
   */
  private static class RFWVOUpdatePending<RFWVOO> {

    /**
     * Caminho desde o VO base até a o {@link #entityVO}.
     */
    private final String path;
    /**
     * EntityVO que foi persistido sem completar a FK esperando o objeto da FK ser persistido.
     */
    private final RFWVO entityVO;
    /**
     * Propriedade da referência que foi definida como NULL para que o objeto pudesse ser persistido.
     */
    private final String property;
    /**
     * VO que deve ganhar o ID até o final da persistência, e ser redefinido em {@link #property} do {@link #entityVO} para terminar a persistência.
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
     * # propriedade da referência que foi definida como NULL para que o objeto pudesse ser persistido.
     *
     * @return the propriedade da referência que foi definida como NULL para que o objeto pudesse ser persistido
     */
    public String getProperty() {
      return property;
    }

    /**
     * # vO que deve ganhar o ID até o final da persistência, e ser redefinido em {@link #property} do {@link #entityVO} para terminar a persistência.
     *
     * @return the vO que deve ganhar o ID até o final da persistência, e ser redefinido em {@link #property} do {@link #entityVO} para terminar a persistência
     */
    public RFWVO getFieldValueVO() {
      return fieldValueVO;
    }

    /**
     * # caminho desde o VO base até a o {@link #entityVO}.
     *
     * @return the caminho desde o VO base até a o {@link #entityVO}
     */
    public String getPath() {
      return path;
    }
  }

  /**
   * Dialeto utilizado na criação dos SQLs.<br>
   * Valor Padrão MySQL.
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
   * Schema a ser utilizado no SQL. Se não for definido, será utilizado o schema que estiver definido na Entidade.
   */
  private final String schema;

  /**
   * Interface utilizada pela aplicação apra resolver informações sobre a entidade.
   */
  private final DAOResolver resolver;

  /**
   * Cria um RFWDAO que força a utilização de um determinado Schema, ao invés de utilizar o schema da sessão do usuário. <br>
   * Este construtor permite passar um DataSource específico. Podendo inclusive ser implementado manualmente para retornar conexões com o banco de dados de forma Local.<br>
   * <br>
   * <b>Atenção:</b> Note que algumas informações, com o schema a ser utilizado, podem ser definidas de várias maneiras: na Entidade, no {@link RFWDAO}, pelo {@link DAOResolver}.<Br>
   * Nesses casos, o {@link RFWDAO} usará a informação conforme disponibilidade de acordo com a seguinte hierarquia:
   * <li>solicita {@link DAOResolver}, se este não existir ou retornar nulo;
   * <li>solicita informação do RFWDAO, se este não tiver a informação ou estiver nula;
   * <li>Verificamos a definição da annotation {@link br.eng.rodrigogml.rfw.base.dao.annotations.dao.RFWDAO} da entidade. Se esta não tiver, resultará em Exception
   *
   * @param type Objeto que será manipulado no banco de dados.
   * @param schema Schema a ser utilizado.
   * @param ds DataSource responsável por entregar conexões sob demanda.
   * @param dialect Defina o dialeto do banco de dados. Embora o comando SQL seja único, cada banco possui diferenças que interferem no funcionamento da classe.
   * @param resolver interface para deixar o {@link RFWDAO} mais dinâmico. Entre as informações está o próprio Schema e tabela de cada entidade. Caso nenhum {@link DAOResolver} seja passado, ou este retorne nulo nas suas informações, as informações passardas no RFWDAO passam a ser utilizadas.
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
   * @param ids ID do objeto a ser excluído
   * @throws RFWException
   *           <li>RFW_ERR_000006 - Critical em caso de falha de Constraint.
   */
  public void delete(Long... ids) throws RFWException {
    // A exclusão é focada apenas na exclusão do objeto principal, uma vez que a restição quando o objeto está em uso ou de objetos de composição deve estar implementada adequadamente no banco de dados.
    final DAOMap map = createDAOMap(this.type, null);

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createDeleteStatement(conn, map, "", dialect, ids)) {
      stmt.executeUpdate();
    } catch (java.sql.SQLIntegrityConstraintViolationException e) {
      // Se a deleção falha por motivos de constraints é possível que o objeto não possa ser apagado. Neste caso há métodos do CRUD que ao invés de excluir o objeto o desativam, por isso enviamos uma exception diferente
      throw new RFWCriticalException("RFW_ERR_000006", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
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
   * Método utilizado para persistir um objeto. Objetos com ID serão atualizados, objetos sem ID é considerado para inserção.
   *
   * @param vo Objeto a ser persistido.
   * @return Objeto com todos os IDs criados.
   * @throws RFWException
   */
  public VO persist(VO vo) throws RFWException {
    return persist(vo, false);
  }

  /**
   * Método utilizado para persistir um objeto. Objetos com ID serão atualizados, objetos sem ID é considerado para inserção.
   *
   * @param vo Objeto a ser persistido.
   * @param ignoreFullLoaded Permite ignorar a verificação se um objeto que será persistido não foi recuperado completamente para atualização. Essa opção só deve ser utilizada em casos muito específicos e preferencialmente quando for possível aplicar outra solução, não utilizar essa, utilizar o {@link #findForUpdate(Long, String[])} sempre que possível.
   * @return Objeto com todos os IDs criados.
   * @throws RFWException
   */
  @SuppressWarnings("deprecation")
  public VO persist(VO vo, boolean ignoreFullLoaded) throws RFWException {
    boolean isNew = vo.getId() == null || vo.isInsertWithID();

    // Para garantir que não vamos estragar objetos pq o desenvolvedor está enviando objetos incompletos para persistência (fazendo com que o RFWDAO exclusi composições e associações, por exemplo) obrigados que o objeto sendo persistido tenha sido obtido através "findForUpdate()" ou similares.
    // Esta é uma medida de segurança para evitar que dados sejam estragador por alguma parte do sistema que não tenha sido atualizada depois que a estrutura de algum objeto tenha sido alterada.
    // NÃO REMOVER, NEM NUNCA DEFINIR MANUALMENTE O VALOR DE ISFULLLOADED FORA DO RFWDAO!!!
    if (!ignoreFullLoaded && !isNew && !vo.isFullLoaded()) throw new RFWCriticalException("O RFWDAO só aceita persistir objetos que foram completamente carregados para edição!");

    final String[] updateAttributes = RUReflex.getRFWVOUpdateAttributes(vo.getClass());
    final DAOMap map = createDAOMap(this.type, updateAttributes);

    final HashMap<String, VO> persistedCache = new HashMap<>(); // Cache para armazenas os objetos que já foram persistidos. Evitando assim cair em loop ou múltiplas atualizações no banco de dados.

    VO originalVO = null;
    if (!isNew) originalVO = findForUpdate(vo.getId(), null);

    HashMap<RFWVO, List<RFWVOUpdatePending<RFWVO>>> updatePendings = new HashMap<RFWVO, List<RFWVOUpdatePending<RFWVO>>>();
    persist(ds, map, isNew, vo, originalVO, "", persistedCache, null, 0, updatePendings, dialect);

    if (updatePendings.size() > 0) {
      for (List<RFWVOUpdatePending<RFWVO>> pendList : updatePendings.values()) {
        for (RFWVOUpdatePending<RFWVO> pendBean : pendList) {
          if (pendBean.getFieldValueVO().getId() == null) {
            throw new RFWCriticalException("Falha ao completar os objetos pendentes! Mesmo deixando para atualizar a referência depois do objeto persistido, alguns objetos continuaram sem IDs para validar as FKs.");
          }
          updateInternalFK(ds, map, pendBean.getPath(), pendBean.getProperty(), pendBean.getEntityVO().getId(), pendBean.getFieldValueVO().getId(), dialect);
        }
      }
    }
    vo.setInsertWithID(false); // Garante que o objeto não vai retornar com a flag em true. Um objeto que tenha ID mas que tenha essa flag em true é considerado pelo sistema como um objeto que não está no banco de dados.
    return vo;
  }

  /**
   * Este método permite que apenas os atributos passados sejam atualizados no banco de dados.<br>
   * O método produt o objeto no banco de acordo com sua classe e 'id' definidos, e copia os valores dos atributos definidos em 'attributes' do VO recebido para o objeto obtido do banco de dados, garantindo assim que apenas os valores selecionados serão atualizados.<Br>
   * <bR>
   * <B>Observação de Usos:</b> Propriedades aninhadas podem ser utilizadas, mas apenas em casos bem específicos:
   * <ul>
   * <li>Cuidado ao definir propriedades aninhadas, este método não inicaliza os objetos se eles vierem nulos do banco de dados, pois nesses casos é necessário enviar o objeto completo e validado.
   * <li>Apenas o objeto principal será persistido no banco de dados, assim, definir propriedades aninhadas em objetos com relacionamento de associação ou parent_association, por exemplo, não fará com que o valor seja atualizado.
   * </ul>
   *
   * @param vo Objeto com o id de identificação e os valores que devem ser atualizados.
   * @param attributes Array com os atributos que devem ser copiados para o objeto original antes de ser persistido.
   * @return
   * @throws RFWException
   * @Deprecated Este método foi criado para retrocompatibilidade com o BIS2 e deve ser apagado em breve. A alternativa desse método é utilizar o {@link #massUpdate(Map, RFWMO)} com um filtro pelo ID.<br>
   *             Ou podemos criar um novo método chamado algo como 'simpleUpdate' ou 'uniqueUpdate', que ao invés do MO receba o ID direto do objeto (e internamente direciona o para o massUpdate.
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
      throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. A entidade já veio com o ID definido para inserção.", new String[] { entityVO.getClass().getCanonicalName() });
    }
    if (isNew || !persistedCache.containsKey(entityVO.getClass().getCanonicalName() + "." + entityVO.getId())) {
      int parentCount = 0;
      boolean needParent = false; // Flag para indicar se encontramos algum PARENT_ASSOCIATION. Se o objeto tiver algum objeto com relacionamento do tipo Parent, torna-se obrigatório ter um parent deifnido
      // ===> TRATAMENTO DO RELACIONAMENTO ANTES DE INSERIR O OBJETO <===
      for (Field field : entityVO.getClass().getDeclaredFields()) {
        final RFWMetaRelationshipField ann = field.getAnnotation(RFWMetaRelationshipField.class);
        if (ann != null) {
          // Verificamos o tipo de relacionamento para validar e saber como proceder.
          switch (ann.relationship()) {
            case WEAK_ASSOCIATION:
              // Nada para fazer, esse tipo de associação é como se não existisse para o RFWDAO.
              break;
            case PARENT_ASSOCIATION: {
              needParent = true;
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());

              // DELETE: Atributos de parentAssociation não há nada para fazer em relação a exclusão, já que quem nunca excluímos o pai, pelo contrário, é ele quem nos excluí.
              // PERSISTENCE: nada a fazer com o objeto pai além da validação abaixo
              // VALIDA: Cada objeto de composição só pode ter 1 pai (Um objeto pode ser reutilizado como filho de outro objeto, mas ele só pode ter um objeto pai definido).
              if (fieldValue != null) parentCount++;
              if (parentCount > 1) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Encontramos mais de um relacionamento marcado como \"Parent Association\". Cada objeto de composição só pode ter 1 pai.", new String[] { entityVO.getClass().getCanonicalName() });

              // VALIDA: Se o objeto pai for obrigatório, se já tem ID
              if (fieldValue == null) {
                // Parent Association se for obrigatório
                if (ann.required()) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associação de pai com objeto nulo ou sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
              } else {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  RFWVO fieldValueVO = (RFWVO) fieldValue;
                  // Nos casos de Parent_Association, não precisamos fazer nada pq o pai já deve ter sido inserido e ter o seu ID pronto antes do filho ser chamado para inserção. Só validamos isso
                  if (fieldValueVO.getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associação de pai com objeto nulo ou sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                } else {
                  // Parent Association não pode ter nada que não seja um RFWVO
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
            case INNER_ASSOCIATION: {
              // VALIDAÇÃO: No caso de INNER_ASSOCIATION, ou o atributo column ou columnMapped devem estar preenchidos
              if ("".equals(getMetaRelationColumnMapped(field, ann)) && "".equals(getMetaRelationColumn(field, ann))) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' está marcado como relacionamento 'Inner Association', este tipo de relacionamento deve ter os atirbutos 'column' ou 'columnMapped' preenchidos.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });

              // DELETE: quando o ID está neste objeto, sendo ele excluído ou a associação desfeita o ID tudo se resolve ao excluir ou atualizar este objeto. No caso de estar na tabela da contraparte, vamos atualizar ela depois que excluírmos esse objeto.
              // PERSISTÊNCIA: na persistência, por ser um objeto que está sendo persistido agora, pode ser que já tenhamos o ID, pode ser que não. Se já tiver o ID, deixa seguir, se não tiver, vamos limpar a associação para que se possa inserir o objeto sem a associação. e colocar o objeto na lista de pendências para atualizar a associação depois que tudo tiver sido persistido.
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
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case COMPOSITION: {
              // PERSISTÊNCIA: Em caso de composição, não fazemos nada aqui no pré-processamento, pois os objetos compostos serão persistidos depois do objeto pai.
              // DELETE: Relacionamento de Composição, precisamos verificar se ele existia antes e deixou de existir, ou em caso de 1:N verifica quais objetos deixaram de existir.
              // ATENÇÃO: Não aceita as coleções nulas pq, por definição, objeto nulo indica que não foi recuperado, a ausência de objetos relacionados deve ser sempre simbolizada por uma lista vazia.
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    RFWVO fieldValueVO = (RFWVO) fieldValue;
                    RFWVO fieldValueVOOrig = (RFWVO) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if (fieldValueVOOrig != null && (fieldValueVO == null || fieldValueVO.getId() == null)) {
                      // Se o objeto no banco existir e o objeto atual for diferente ou não tiver ID, temos de excluir o objeto atual pq o objeto mudou.
                      delete(ds, daoMap, fieldValueVOOrig, RUReflex.addPath(path, field.getName()), dialect);
                    }
                  }
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    List list = (List) fieldValue;
                    List listOrig = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if ((list == null || list.size() == 0) && (listOrig != null && listOrig.size() > 0)) {
                      // Se não temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
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
                      // Se não temos lista agora, e já não tinhamos, nada a fazer. O IF só previne cair no else e lançar a Exception de "prevenção de falha de lógica".
                    } else {
                      throw new RFWCriticalException("Falha ao detectar a condição de comparação entre listas do novo objeto e do objeto anterior! Atributo '${0}' da Classe '${1}'.", new String[] { field.getName(), entityVO.getClass().getCanonicalName() });
                    }
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    Map hash = (Map) fieldValue;
                    Map hashOrig = (Map) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if (hash.size() == 0 && hashOrig.size() > 0) {
                      // Se não temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
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
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              } else {
                // Se não existe no objeto atual, verificamos se existe no objeto original
                if (entityVOOrig != null) {
                  final Object fieldValueOrig = RUReflex.getPropertyValue(entityVOOrig, field.getName());
                  if (fieldValueOrig != null) {
                    if (RFWVO.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Se o objeto no banco existir e o objeto atual não, temos de excluir o objeto atual pq a composição mudou.
                      delete(ds, daoMap, (VO) fieldValueOrig, RUReflex.addPath(path, field.getName()), dialect);
                    } else if (List.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Se não temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
                      for (Object item : (List) fieldValueOrig) {
                        delete(ds, daoMap, (VO) item, RUReflex.addPath(path, field.getName()), dialect);
                      }
                    } else if (Map.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Se não temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
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
              // PERSISTÊNCIA: Em caso de composição, não fazemos nada aqui no pré-processamento, pois os objetos compostos serão persistidos depois do objeto pai.
              // DELETE: Relacionamento de Composição, precisamos verificar se ele existia antes e deixou de existir. Se ele deixou de existir, precisamos excluir todas sua hierarquia.
              // ATENÇÃO: Não aceita as coleções nulas pq, por definição, objeto nulo indica que não foi recuperado, a ausência de objetos relacionados deve ser sempre simbolizada por uma lista vazia.
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  throw new RFWValidationException("Encontrado a definição 'COMPOSITION_TREE' em um relacionamento 1:1. Essa definição só pode ser utilizado em coleções para indicar os 'filhos' do relacionamento hierarquico. Classe: ${0} / Field: ${1} / FieldClass: ${2}.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    List list = (List) fieldValue;
                    List listOrig = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if ((list == null || list.size() == 0) && (listOrig != null && listOrig.size() > 0)) {
                      // Se não temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
                      for (Object item : listOrig) {
                        String destPath = RUReflex.addPath(path, field.getName());
                        // Se não tivermos o caminho temos de completar dianimicamente no DAOMap
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
                      // Se não temos lista agora, e já não tinhamos, nada a fazer. O IF só previne cair no else e lançar a Exception de "prevenção de falha de lógica".
                    } else {
                      throw new RFWCriticalException("Falha ao detectar a condição de comparação entre listas do novo objeto e do objeto anterior! Atributo '${0}' da Classe '${1}'.", new String[] { field.getName(), entityVO.getClass().getCanonicalName() });
                    }
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  if (entityVOOrig != null) {
                    Map hash = (Map) fieldValue;
                    Map hashOrig = (Map) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if (hash.size() == 0 && hashOrig.size() > 0) {
                      // Se não temos mais objetos relacionados, mas antes tinhamos, apagamos todos os itens da lista anterior.
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
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case ASSOCIATION: {
              // VALIDAÇÃO: No caso de associação, ou o atributo column ou columnMapped devem estar preenchidos
              if ("".equals(getMetaRelationColumnMapped(field, ann)) && "".equals(getMetaRelationColumn(field, ann))) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' está marcado como relacionamento 'Association', este tipo de relacionamento deve ter os atirbutos 'column' ou 'columnMapped' preenchidos.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });

              // DELETE: nos casos de associação, quando o ID está na nossa tabela, ele será definido como null ao atualizar o objeto e não devemos apagar a contra-parte. No caso do ID estar na tabela da contra-parte, vamos defini-lo como nulo depois do persistir o objeto atualizado
              // PERSISTÊNCIA: Nos casos de associação é esperado que o objeto associado já tenha um ID definido, já que é um objeto a parte
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  VO fieldValueVO = (VO) fieldValue;
                  if (fieldValueVO.getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associação sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  List list = (List) fieldValue;
                  for (Object item : list) {
                    if (((VO) item).getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associação sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  Map map = (Map) fieldValue;
                  for (Object item : map.values()) {
                    if (((VO) item).getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associação sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case MANY_TO_MANY:
              // DELETE: os relacionamentos N:N serão excluídos depois da atualização do objeto
              // PERSISTÊNCIA: Nos casos de ManyToMany a coluna de FK não está na tabela do objeto (e sim na tabela de joinAlias). Por isso tudo o que temos que fazer é validar se todos os objetos tem um ID para a posterior inserção.
              // PERSISTÊNCIA: Note que ManyToMany deve sempre estar dentro de algum tipo de coleção/lista/hash/etc por ser múltiplos objetos.
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue == null) {
                // Por ser esperado sempre uma Lista nas associações ManyToMany, um objeto recebido nulo é um erro, já que nulo indica que não foi carregado enquanto que uma coleção vazia indica a ausência de associações.
                throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. No atributo '${1}' recebemos uma coleção vazia. A ausência de relacionamento deve sempre ser indicada por uma coleção vazia, o atributo nulo é indicativo de que ele não foi carredo do banco de dados.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
              } else {
                if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  List list = (List) fieldValue;
                  for (Object item : list) {
                    if (((VO) item).getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associação sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  Map hash = (Map) fieldValue;
                  for (Object item : hash.values()) {
                    if (((VO) item).getId() == null) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O atributo '${1}' trouxe uma associação sem ID!", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
              break;
          }
        }
        final RFWMetaCollectionField colAnn = field.getAnnotation(RFWMetaCollectionField.class);
        if (colAnn != null) {
          // No caso de lista, temos de excluir todos os objetos anteriores do banco para inserir os novos depois. Não temos como comparar pq não utilizamos IDs nesses objetos. Assim, se o objeto original existir excluímos todos os itens associados anteriormente de uma única vez.
          if (entityVOOrig != null) {
            deleteCollection(ds, daoMap, entityVOOrig, "@" + RUReflex.addPath(path, field.getName()), dialect);
          }
        }
      }

      if (needParent && parentCount == 0) throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Há relacionamentos do tipo 'PARENT_ASSOCIATION', o que indica que o objeto é dependente de outro, mas nenhum relacionamento desse tipo foi definido!", new String[] { entityVO.getClass().getCanonicalName() });

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
                throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. O ID não foi retornado pelo banco de dados. Verifique se a coluna 'id' gera as chaves automaticamente.", new String[] { entityVO.getClass().getCanonicalName() });
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
        throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
      }

      // ===> PROCESSAMENTO DOS RELACIONAMENTOS PÓS INSERÇÃO DO OBJETO
      for (Field field : entityVO.getClass().getDeclaredFields()) {
        final RFWMetaRelationshipField ann = field.getAnnotation(RFWMetaRelationshipField.class);
        if (ann != null) {
          // Verificamos o tipo de relacionamento para validar e saber como proceder.
          switch (ann.relationship()) {
            case WEAK_ASSOCIATION:
              // Nada para fazer, esse tipo de associação é como se não existisse para o RFWDAO.
              break;
            case ASSOCIATION:
              // No caso de associação e a FK estar na tabela do outro objeto, temos atualizar a coluna do outro objeto. (Se estiver na tabela do objeto sendo editado o valor já foi definido)
              if (!"".equals(getMetaRelationColumnMapped(field, ann))) {
                // Verificamos se houve alteração entre a associação atual e a associação existente no banco de dados para saber se precisamos atualizar a tabels
                final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
                if (fieldValue != null) { // Atualmente temos um relacionamento
                  if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                    RFWVO fieldValueVO = (RFWVO) fieldValue;
                    RFWVO fieldValueVOOrig = null;
                    if (entityVOOrig != null) fieldValueVOOrig = (RFWVO) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                    if (fieldValueVOOrig != null && !fieldValueVO.getId().equals(fieldValueVOOrig.getId())) {
                      // Se também temos um relacionamento no VO original e eles tem IDs diferentes, precisamos remover a associação do objeto anterior antes de incluir a nova associação (se tem o mesmo ID não precisamos fazer nada pois já estão certos)
                      updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueVOOrig.getId(), null, dialect); // Exclui a associação do objeto anterior
                    }
                    // Agora que já removemos as associações do objeto que não estão mais em uso, vamos atualizar as novas associações.
                    updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueVO.getId(), entityVO.getId(), dialect); // Inclui a associação do novo Objeto
                  } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                    Map fieldValueMap = (Map) fieldValue;
                    Map fieldValueMapOrig = null;
                    if (entityVOOrig != null) fieldValueMapOrig = (Map) RUReflex.getPropertyValue(entityVOOrig, field.getName());

                    if (fieldValueMapOrig != null && fieldValueMapOrig.size() > 0) {
                      // Se também temos um relacionamento no VO original, iteramos seus objetos para comparação...
                      for (Object key : fieldValueMapOrig.keySet()) {
                        RFWVO fieldValueVOOrig = (RFWVO) fieldValueMapOrig.get(key);
                        RFWVO fieldValueVO = (RFWVO) fieldValueMap.get(key);
                        if (fieldValueVO == null || !fieldValueVO.getId().equals(fieldValueVOOrig.getId())) {
                          // ..., temos o objeto para as mesma chavez, mas eles tem IDs diferentes, precisamos remover a associação antiga (a nova associação é feita depois) (se tem o mesmo ID não precisamos fazer nada pois já estão certos)
                          updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueVOOrig.getId(), null, dialect); // Exclui a associação do objeto anterior na tabela
                        }
                      }
                    }
                    // Tendo ou não removido associações dos objetos que não estão mais associados, atualizamos os novos objetos associados
                    for (Object obj : fieldValueMap.values()) {
                      RFWVO fieldValueVO = (RFWVO) obj;
                      updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueVO.getId(), entityVO.getId(), dialect); // Atualiza a associação na tabela do objeto associado.
                    }
                  } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                    List list = (List) fieldValue;
                    List listOriginal = null;
                    if (entityVOOrig != null) listOriginal = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());

                    if (listOriginal != null && listOriginal.size() > 0) {
                      // Se também temos um relacionamento no VO original, iteramos seus objetos para comparação...
                      for (Object itemOriginal : listOriginal) {
                        RFWVO itemVOOrig = (RFWVO) itemOriginal;
                        RFWVO itemVO = null;
                        if (list != null) {
                          // Se temos uma lista do objeto atual, vamos tentar encontrar o objeto para atualização
                          for (Object item : list) {
                            if (itemVOOrig.getId().equals(((VO) item).getId())) { // ItemOriginal sempre tem um ID pois veio do banco de dados.
                              itemVO = (VO) item;
                              break;
                            }
                          }
                        }

                        if (itemVO == null || !itemVOOrig.getId().equals(itemVO.getId())) {
                          // ..., temos o objeto em ambas a lista, mas eles tem IDs diferentes, precisamos remover a associação antiga (a nova associação é feita depois) (se tem o mesmo ID não precisamos fazer nada pois já estão certos)
                          updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), itemVOOrig.getId(), null, dialect); // Exclui a associação do objeto anterior na tabela
                        }
                      }
                    }
                    // Tendo ou não removido associações dos objetos que não estão mais associados, atualizamos os novos objetos associados
                    for (Object item : list) {
                      RFWVO itemVO = (RFWVO) item;
                      updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), itemVO.getId(), entityVO.getId(), dialect); // Atualiza a associação na tabela do objeto associado.
                    }
                  } else {
                    throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                  }
                } else {
                  // Se não temos uma associação no objeto atual, temos que remover da antiga caso exista
                  Object fieldValueOrig = null;
                  if (entityVOOrig != null) fieldValueOrig = RUReflex.getPropertyValue(entityVOOrig, field.getName());
                  if (fieldValueOrig != null) {
                    if (RFWVO.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      RFWVO fieldValueOrigVO = (RFWVO) fieldValueOrig;
                      updateExternalFK(ds, daoMap, RUReflex.addPath(path, field.getName()), fieldValueOrigVO.getId(), null, dialect); // Exclui a associação na tabela do objeto anterior
                    } else if (List.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Caso no objeto original tenha uma list lançamos erro. Pois o objeto sendo persistido não deve ter as collections nulas e sim vazias para indicar a ausência de associações. Uma collection nula provavelmente indica que o objeto não foi bem inicializado, ou mal recuperado do banco em caso de atualização.
                      throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. No atributo '${1}' recebemos uma coleção vazia. A ausência de relacionamento deve sempre ser indicada por uma coleção vazia, o atributo nulo é indicativo de que ele não foi carredo do banco de dados.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                    } else if (Map.class.isAssignableFrom(fieldValueOrig.getClass())) {
                      // Caso no objeto original tenha uma hash lançamos erro. Pois o objeto sendo persistido não deve ter as collections nulas e sim vazias para indicar a ausência de associações. Uma collection nula provavelmente indica que o objeto não foi bem inicializado, ou mal recuperado do banco em caso de atualização.
                      throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. No atributo '${1}' recebemos uma coleção vazia. A ausência de relacionamento deve sempre ser indicada por uma coleção vazia, o atributo nulo é indicativo de que ele não foi carredo do banco de dados.", new String[] { entityVO.getClass().getCanonicalName(), field.getName() });
                    }
                  }
                }
              }
              break;
            case COMPOSITION: {
              // PERSISTÊNCIA: Em caso de composição, temos agora que persistir todos os objetos filhos
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  VO fieldValueVOOrig = null;
                  if (entityVOOrig != null) fieldValueVOOrig = (VO) RUReflex.getPropertyValue(entityVOOrig, field.getName());
                  // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composição não podem ter ID definido antes do próprio pai, provavelmente isso é um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composição novo (em insert).
                  persist(ds, daoMap, (isNew || ((VO) fieldValue).getId() == null), (VO) fieldValue, fieldValueVOOrig, RUReflex.addPath(path, field.getName()), persistedCache, null, 0, updatePendings, dialect);
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  List list = (List) fieldValue;
                  List listOriginal = null;
                  if (entityVOOrig != null) listOriginal = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());

                  // Se é uma lista, verificamos se tem o atributo "sortColumn" definido na Annotation. Nestes casos temos de criar esse atributo para ser salvo junto
                  String sColumn = null;
                  if (!"".equals(ann.sortColumn())) sColumn = ann.sortColumn();

                  int countIndex = 0; // Contador de indice. Usado para saber o índice do item na lista. Utilizado quando o sortColumn é definido para garantir a ordem da lista.
                  for (Object item : list) {
                    VO itemVO = (VO) item;
                    VO itemVOOrig = null;
                    if (listOriginal != null) {
                      // Se temos uma lista do objeto original, vamos tentar encontrar o objeto para passar como objeto original para comparação
                      for (Object itemOriginal : listOriginal) {
                        if (((VO) itemOriginal).getId().equals(itemVO.getId())) { // ItemOriginal sempre tem um ID pois veio do banco de dados.
                          itemVOOrig = (VO) itemOriginal;
                          break;
                        }
                      }
                    }

                    // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composição não podem ter ID definido antes do próprio pai, provavelmente isso é um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composição novo (em insert).
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
                    // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composição não podem ter ID definido antes do próprio pai, provavelmente isso é um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composição novo (em insert).
                    persist(ds, daoMap, (isNew || itemVO.getId() == null), itemVO, itemVOOrig, RUReflex.addPath(path, field.getName()), persistedCache, null, 0, updatePendings, dialect);
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case COMPOSITION_TREE: {
              // PERSISTÊNCIA: Em caso de composição de árvore, temos agora que persistir todos os objetos filhos
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (RFWVO.class.isAssignableFrom(fieldValue.getClass())) {
                  throw new RFWValidationException("Encontrado a definição 'COMPOSITION_TREE' em um relacionamento 1:1. Essa definição só pode ser utilizado em coleções para indicar os 'filhos' do relacionamento hierarquico. Classe: ${0} / Field: ${1} / FieldClass: ${2}.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                } else if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  List list = (List) fieldValue;
                  List listOriginal = null;
                  if (entityVOOrig != null) listOriginal = (List) RUReflex.getPropertyValue(entityVOOrig, field.getName());

                  // Se é uma lista, verificamos se tem o atributo "sortColumn" definido na Annotation. Nestes casos temos de criar esse atributo para ser salvo junto
                  String sColumn = null;
                  if (!"".equals(ann.sortColumn())) sColumn = ann.sortColumn();

                  int countIndex = 0; // Contador de indice. Usado para saber o índice do item na lista. Utilizado quando o sortColumn é definido para garantir a ordem da lista.
                  for (Object item : list) {
                    VO itemVO = (VO) item;
                    VO itemVOOrig = null;
                    if (listOriginal != null) {
                      // Se temos uma lista do objeto original, vamos tentar encontrar o objeto para passar como objeto original para comparação
                      for (Object itemOriginal : listOriginal) {
                        if (((VO) itemOriginal).getId().equals(itemVO.getId())) { // ItemOriginal sempre tem um ID pois veio do banco de dados.
                          itemVOOrig = (VO) itemOriginal;
                          break;
                        }
                      }
                    }

                    // Antes de passar para os objetos filhos em "esquema de árvore". Precisamos completar o DAOMap, isso pq quando ele é feito limitamos o mapeamento de estruturas hierarquicas por tender ao infinito. Vamos duplicando o mapeamento aqui, dinamicamente
                    String destPath = RUReflex.addPath(path, field.getName());
                    // System.out.println(dumpDAOMap(daoMap));
                    // Se não tivermos o caminho temos de completar dianimicamente no DAOMap
                    daoMap.createMapTableForCompositionTree(path, destPath, "id", getMetaRelationColumnMapped(field, ann));
                    // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composição não podem ter ID definido antes do próprio pai, provavelmente isso é um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composição novo (em insert).
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
                    // Passamos isNew como true sempre que o objeto atual (objeto pai) for novo, isso pq objetos de composição não podem ter ID definido antes do próprio pai, provavelmente isso é um erro. No entanto, o pai pode ser "velho" (em update) e o objeto da composição novo (em insert).
                    persist(ds, daoMap, (isNew || itemVO.getId() == null), itemVO, itemVOOrig, RUReflex.addPath(path, field.getName()), persistedCache, null, 0, updatePendings, dialect);
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
            }
              break;
            case INNER_ASSOCIATION:
              // Neste caso não há nada para fazer neste ponto.
              break;
            case PARENT_ASSOCIATION:
              // No caso de um relacionamento com o objeto pai, não temos de fazer nada, pois tanto o pai quando o ID do pai já deve ter sido persistido
              break;
            case MANY_TO_MANY: {
              // Os relacionamentos ManyToMany precisam ter os inserts da tabela de Join realizados para "linkar" os dois objetos
              final Object fieldValue = RUReflex.getPropertyValue(entityVO, field.getName());
              if (fieldValue != null) {
                if (List.class.isAssignableFrom(fieldValue.getClass())) {
                  for (Object item : (List) fieldValue) {
                    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createManyToManySelectStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) item, dialect); ResultSet rs = stmt.executeQuery()) {
                      if (!rs.next()) {
                        // Se não tem um resultado próximo, criamos a inserção, se não deixa quieto que já foi feito
                        try (PreparedStatement stmt2 = DAOMap.createManyToManyInsertStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) item, dialect)) {
                          stmt2.executeUpdate();
                        }
                      }
                    } catch (Throwable e) {
                      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
                    }
                  }
                } else if (Map.class.isAssignableFrom(fieldValue.getClass())) {
                  for (Object item : ((Map) fieldValue).values()) {
                    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createManyToManySelectStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) item, dialect); ResultSet rs = stmt.executeQuery()) {
                      if (!rs.next()) {
                        // Se não tem um resultado próximo, criamos a inserção, se não deixa quieto que já foi feito
                        try (PreparedStatement stmt2 = DAOMap.createManyToManyInsertStatement(conn, daoMap, RUReflex.addPath(path, field.getName()), entityVO, (VO) item, dialect)) {
                          stmt2.executeUpdate();
                        }
                      }
                    } catch (Throwable e) {
                      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
                    }
                  }
                } else {
                  throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValue.getClass().getCanonicalName() });
                }
              }
              // Se existir uma lista no objeto original, precisamos apagar todos os mapeamentos que não existem mais, caso contrário as desassociações não deixarão de existir
              if (entityVOOrig != null) {
                final Object fieldValueOrig = RUReflex.getPropertyValue(entityVOOrig, field.getName());
                if (fieldValueOrig != null) {
                  if (List.class.isAssignableFrom(fieldValueOrig.getClass())) {
                    List listOrig = (List) fieldValueOrig;
                    for (Object itemOrig : listOrig) {
                      boolean found = false;
                      // Vamos iterar a lista de objetos atual para ver se encontramos o objeto. se não encontrar excluimos o link entre os objetos
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
                          throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
                        }
                      }
                    }
                  } else if (Map.class.isAssignableFrom(fieldValueOrig.getClass())) {
                    Map hashOrig = (Map) fieldValueOrig;
                    for (Object itemOrig : hashOrig.values()) {
                      boolean found = false;
                      // Vamos iterar a lista de objetos atual para ver se encontramos o objeto. se não encontrar excluimos o link entre os objetos
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
                          throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
                        }
                      }
                    }
                  } else {
                    throw new RFWCriticalException("Falha ao persistir o objeto '${0}'. Não é possível persistir o atributo '${1}' por ser do tipo '${2}'.", new String[] { entityVO.getClass().getCanonicalName(), field.getName(), fieldValueOrig.getClass().getCanonicalName() });
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
              throw new RFWCriticalException("O RFWDAO não sabe persistir uma RFWMetaCollectionField com o objeto do tipo '" + colValue.getClass().getCanonicalName() + "'");
            }
          }
        }
      }
    }

  }

  /**
   * Recupera a definição do atributo "column" da Annotation {@link RFWMetaRelationshipField}, com a possibilidade de intervenção do {@link DAOResolver}.
   *
   * @param field Field do objeto que estamos tratando, e procurando o valor da coluna para terminar o mapeamento.
   * @param ann referência para a {@link RFWMetaRelationshipField} encontrada.
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
   * Recupera a definição do atributo "column" da Annotation {@link RFWMetaRelationshipField}, com a possibilidade de intervenção do {@link DAOResolver}.
   *
   * @param field Field do objeto que estamos tratando, e procurando o valor da coluna para terminar o mapeamento.
   * @param ann referência para a {@link RFWMetaRelationshipField} encontrada.
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
   * Recupera a definição do atributo "columnMapped" da Annotation {@link RFWMetaRelationshipField}, com a possibilidade de intervenção do {@link DAOResolver}.
   *
   * @param field Field do objeto que estamos tratando, e procurando o valor da coluna para terminar o mapeamento.
   * @param ann referência para a {@link RFWMetaRelationshipField} encontrada.
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
   * Atualiza uma associação quando a coluna de FK está na tabela do objento associado, e não na tabela da entidade sendo atualizada.
   *
   * @param ds Data Source da conexão.
   * @param map Mapeamento Objeto x Tabelas
   * @param path Caminho completo do atributo que contem o objeto associado.
   * @param id ID do objeto associado (para identificação na tabela que será atualizada).
   * @param newID ID que será colocado na tabela. Normalmente o ID do objeto sendo editado pelo RFWDAO ou null caso estejamos eliminando a associação.
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
   * Atualiza uma associação quando a coluna de FK está na tabela do próprio objeto (sem alterar mais dada do objeto.
   *
   * @param ds Data Source da conexão.
   * @param map Mapeamento Objeto x Tabelas
   * @param path Caminho completo até o objeto que será atualizado.
   * @param property Propriedade do objeto que tem a associação com a FK na própria tabela.
   * @param id ID do objeto (para identificação na tabela que será atualizada).
   * @param newID ID que será colocado na tabela. Normalmente o ID do objeto sendo editado pelo RFWDAO ou null caso estejamos eliminando a associação.
   * @throws RFWException
   */
  private static <VO extends RFWVO> void updateInternalFK(DataSource ds, DAOMap map, String path, String property, Long id, Long newID, SQLDialect dialect) throws RFWException {
    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createUpdateInternalFKStatement(conn, map, path, property, id, newID, dialect)) {
      stmt.executeUpdate();
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao corrigir FK de associação do objeto no banco de dados!", e);
    }
  }

  /**
   * Este método é utilizado para excluir do banco todos os elementos de um atributo anotado com a {@link RFWMetaCollectionField}.
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
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
    }
  }

  /**
   * Busca a entidade a partir do seu ID.
   *
   * @param id ID do objeto a ser encontrado no banco de dados.
   * @param attributes Atributos da entidade que devem ser recuperados.
   * @return Objeto montado caso seja encontrado, null caso contrário.
   * @throws RFWException Lançado caso ocorra algum problema para montar ou obter o objeto
   */
  @SuppressWarnings("unchecked")
  public VO findById(Long id, String[] attributes) throws RFWException {
    if (id == null) throw new NullPointerException("ID can't be null!");

    final DAOMap map = createDAOMap(this.type, attributes);

    // Cria a condição do ID
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
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
    }
    return null;
  }

  /**
   * Busca a entidade a partir do seu ID para Atualização.
   *
   * @param id ID do objeto a ser encontrado no banco de dados.
   * @param attributes Atributos da entidade que devem ser recuperados. Atributos de associação e composição são recuperados automaticamente.
   * @return Objeto montado caso seja encontrado, null caso contrário.
   * @throws RFWException Lançado caso ocorra algum problema para montar ou obter o objeto
   */
  @SuppressWarnings({ "deprecation", "unchecked" }) // attributes = new String[0];
  public VO findForUpdate(Long id, String[] attributes) throws RFWException {
    if (id == null) throw new NullPointerException("ID can't be null!");

    final String[] attForUpdate = RUReflex.getRFWVOUpdateAttributes(type);
    attributes = RUArray.concatAll(new String[0], attributes, attForUpdate);

    final DAOMap map = createDAOMap(this.type, attributes);

    // Cria a condição do ID
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
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
    }
    return null;
  }

  /**
   * Busca uma lista IDs dos VOs baseado em um critério de "search".
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param orderBy Objeto para definir a ordenação da lista
   * @return Lista com os objetos que respeitam o critério estabelecido e na ordem desejada.
   * @throws RFWException Lançado em caso de erro.
   */
  public List<Long> findIDs(RFWMO mo, RFWOrderBy orderBy) throws RFWException {
    return findIDs(mo, orderBy, null, null);
  }

  /**
   * Busca uma lista IDs dos VOs baseado em um critério de "search".
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param orderBy Objeto para definir a ordenação da lista
   * @param offSet Define quantos registros a partir do começo devemos pular (não retornar), por exemplo, um offSet de 0, retorna desde o primeiro (index 0).
   * @param limit Define quantos registros devemos retornar da lista. Define a quantidade e não o index como o "offSet". Se ambos forem combinados, ex: offset = 5 e limit = 10, retorna os registros desde o index 5 até o idnex 15, ou seja da 6ª linha até a 15ª.
   * @return Lista com os objetos que respeitam o critério estabelecido e na ordem desejada.
   * @throws RFWException Lançado em caso de erro.
   */
  public List<Long> findIDs(RFWMO mo, RFWOrderBy orderBy, Integer offSet, Integer limit) throws RFWException {
    // Primeiro vamos buscar apenas os ids do objeto raiz que satisfazem as condições
    String[] moAtt = new String[0];
    if (mo != null) moAtt = RUArray.concatAll(moAtt, mo.getAttributes().toArray(new String[0]));
    if (orderBy != null) moAtt = RUArray.concatAll(moAtt, orderBy.getAttributes().toArray(new String[0]));

    final DAOMap map = createDAOMap(this.type, moAtt);

    try (Connection conn = ds.getConnection()) {
      // conn.setAutoCommit(false);
      try (PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, new String[] { "id" }, false, mo, orderBy, offSet, limit, null, dialect)) {
        stmt.setFetchSize(1000);
        try (ResultSet rs = stmt.executeQuery()) {
          // Ao usar o LinkedHashSet ele não aceita valores iguais (ele os sobrepõe automaticamente na Hash) fazendo com que no final só tenhamos uma lista de objetos distintos.
          // Precisamos utilizar o LinkedHashSet ao invés do habitual HashSet para que ele mantenha a ordem dos objetos na saída.
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
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
    }
  }

  /**
   * Busca a quantidade de itens que uma busca por filtro {@link RFWMO} retornará.<br>
   * Esta busca faz uma query do tipo 'SELECT COUNT(*)...' trazendo do banco apenas o total, sem carregar qualquer outro tipo de objeto, o que melhora a performance quando o desejado é apenas o total de itens.
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @return Long com a quantidade de itens encontrados. Zero se a query retornou vazia.
   * @throws RFWException Lanaçado em caso de erro.
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
   * @return Lista com os valores distintos da coluna/propriedade solicitada. Os objetos tendem a ser equivalente ao tipo de dado no banco de dados. Pois são criados a partir do método .getObject() do ResultSet, e sem intervenção do RFWDAO.
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
   * Busca uma lista de VOs baseado em um critério de "search".
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param orderBy Objeto para definir a ordenação da lista
   * @param attributes Atributos que devem ser recuperados em cada objeto.
   * @return Lista com os objetos que respeitam o critério estabelecido e na ordem desejada.
   * @throws RFWException Lançado em caso de erro.
   */
  public List<VO> findList(RFWMO mo, RFWOrderBy orderBy, String[] attributes) throws RFWException {
    return findList(mo, orderBy, attributes, null, null);
  }

  /**
   * Busca uma lista de VOs baseado em um critério de "search".
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param orderBy Objeto para definir a ordenação da lista
   * @param attributes Atributos que devem ser recuperados em cada objeto.
   * @param offSet Define quantos registros a partir do começo devemos pular (não retornar), por exemplo, um offSet de 0, retorna desde o primeiro (index 0).
   * @param limit Define quantos registros devemos retornar da lista. Define a quantidade e não o index como o "offSet". Se ambos forem combinados, ex: offset = 5 e limit = 10, retorna os registros desde o index 5 até o idnex 15, ou seja da 6ª linha até a 15ª.
   * @return Lista com os objetos que respeitam o critério estabelecido e na ordem desejada.
   * @throws RFWException Lançado em caso de erro.
   */
  @SuppressWarnings("unchecked")
  public List<VO> findList(RFWMO mo, RFWOrderBy orderBy, String[] attributes, Integer offSet, Integer limit) throws RFWException {
    if (mo == null) mo = new RFWMO();
    // Primeiro vamos buscar apenas os ids do objeto raiz que satisfazem as condições
    String[] atts = RUArray.concatAll(new String[0], mo.getAttributes().toArray(new String[0]), attributes);
    if (orderBy != null) atts = RUArray.concatAll(atts, orderBy.getAttributes().toArray(new String[0]));
    final DAOMap map = createDAOMap(this.type, atts);

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, new String[] { "id" }, false, mo, orderBy, offSet, limit, null, dialect); ResultSet rs = stmt.executeQuery()) {
      final LinkedList<Long> ids = new LinkedList<>();
      DAOMapTable mTable = map.getMapTableByPath("");
      while (rs.next()) {
        final long id = getRSInteger(rs, mTable.schema, mTable.table, mTable.alias, "id", dialect);// rs.getLong("id");
        if (!ids.contains(id)) ids.add(id); // Não permite colocar duplicado, dependendo das conexões utilizadas nos LeftJoins, o mesmo ID pode retornar múltiplas vezes
      }

      // Se não temos um ID para procurar, é pq o objeto não foi encontrado, simplesmente retorna a lista vazia
      if (ids.size() == 0) return new LinkedList<>();

      // Com base nos IDs retornados, montar um RFWMO para retornar todos os objetos com os IDs, e neste caso já passamos as colunas que queremos montar no objeto
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
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
    }
  }

  public List<Object[]> findListEspecial(RFWField[] fields, RFWMO mo, RFWOrderBy orderBy, RFWField[] groupBy, Integer offSet, Integer limit) throws RFWException {
    return findListEspecial(fields, mo, orderBy, groupBy, offSet, limit, null);
  }

  public List<Object[]> findListEspecial(RFWField[] fields, RFWMO mo, RFWOrderBy orderBy, RFWField[] groupBy, Integer offSet, Integer limit, Boolean useFullJoin) throws RFWException {
    if (mo == null) mo = new RFWMO();

    // Verificamos todos os atributos que precisamos mapear conforme utilização em cada parte do SQL
    String[] attsMO = mo.getAttributes().toArray(new String[0]);
    String[] attsFields = new String[0];
    if (fields == null) throw new RFWCriticalException("O parâmetro 'fields' não pode ser nulo!");
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

    // Mapeamos todos os objetos necessários
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
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
    }
  }

  /**
   * Busca um objeto através do MatchObject. Note que as condições usadas no MO devem garantir que apenas 1 objeto será retornado, como a busca por um campo definido como Unique. <br>
   * Caso este método encontre mais de um objeto para a mesma busca, um erro crítico será lançado.
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param attributes Atributos que devem ser recuperados em cada objeto.
   * @return Objecto único encontrado
   * @throws RFWException Lançado em caso de erro.
   */
  public VO findUniqueMatch(RFWMO mo, String[] attributes) throws RFWException {
    if (mo == null) mo = new RFWMO();
    // Primeiro vamos buscar apenas os ids do objeto raiz que satisfazem as condições
    final DAOMap map = createDAOMap(this.type, RUArray.concatAll(new String[0], mo.getAttributes().toArray(new String[0]), attributes));

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, new String[] { "id" }, false, mo, null, null, null, null, dialect); ResultSet rs = stmt.executeQuery()) {

      DAOMapTable mTable = map.getMapTableByPath("");
      Long id = null;
      while (rs.next()) {
        final long rsID = getRSLong(rs, mTable.schema, mTable.table, mTable.alias, "id", dialect); // rs.getLong("id");
        // Precisamos verficiar se não é o mesmo ID pq as vezes a consult inclui joins de listas, o que faz com que várias linhas retornem para o mesmo objeto
        if (id != null && id != rsID) throw new RFWCriticalException("Encontrado mais de um objeto pelo método 'findUniqueMatch()'.");
        id = rsID;
      }

      // Se não temos um ID para procurar, é pq o objeto não foi encontrado, simplesmente retorna null
      if (id == null) return null;
      // Com base nos IDs retornados, montar um RFWMO para retornar todos os objetos com os IDs, e neste caso já passamos as colunas que queremos montar no objeto
      return findById(id, attributes);
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
    }
  }

  /**
   * Busca um objeto através do MatchObject para edição. Note que as condições usadas no MO devem garantir que apenas 1 objeto será retornado, como a busca por um campo definido como Unique. <br>
   * Caso este método encontre mais de um objeto para a mesma busca, um erro crítico será lançado.
   *
   * @param mo Match Object para realizar o filtro no banco de dados.
   * @param attributes Atributos que devem ser recuperados em cada objeto.
   * @return Objecto único encontrado
   * @throws RFWException Lançado em caso de erro.
   */
  public VO findUniqueMatchForUpdate(RFWMO mo, String[] attributes) throws RFWException {
    if (mo == null) mo = new RFWMO();
    // Primeiro vamos buscar apenas os ids do objeto raiz que satisfazem as condições
    final DAOMap map = createDAOMap(this.type, RUArray.concatAll(new String[0], mo.getAttributes().toArray(new String[0]), attributes));

    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectStatement(conn, map, new String[] { "id" }, false, mo, null, null, null, null, dialect); ResultSet rs = stmt.executeQuery()) {
      DAOMapTable mTable = map.getMapTableByPath("");
      Long id = null;
      while (rs.next()) {
        final long rsID = getRSLong(rs, mTable.schema, mTable.table, mTable.alias, "id", dialect); // rs.getLong("id");
        // Precisamos verficiar se não é o mesmo ID pq as vezes a consult inclui joins de listas, o que faz com que várias linhas retornem para o mesmo objeto
        if (id != null && id != rsID) throw new RFWCriticalException("Encontrado mais de um objeto pelo método 'findUniqueMatch()'.");
        id = rsID;
      }

      // Se não temos um ID para procurar, é pq o objeto não foi encontrado, simplesmente retorna null
      if (id == null) return null;
      // Com base nos IDs retornados, montar um RFWMO para retornar todos os objetos com os IDs, e neste caso já passamos as colunas que queremos montar no objeto
      return findForUpdate(id, attributes);
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
    }
  }

  /**
   * Método utilizado ler os dados do result set e montar os objetos conforme forem retornados.
   *
   * @param rs ResultSet da consulta no banco de dados
   * @param map mapeamento da consulta.
   * @param cache Cache com os objetos já criados. Utilizado para a recursão do método. Quando chamado de fora do próprio método: passar NULL.
   * @throws RFWException
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private List<RFWVO> mountVO(ResultSet rs, DAOMap map, HashMap<String, RFWVO> cache) throws RFWException {
    try {
      // ATENÇÃO: NÃO TRABALHAMOS COM LinkedList, apesar de ter menos overhead que o ArrayList pq o Glassfish ao tentar colocar o linkedlist na fachada acaba estourando a memória. É um BUG do Glassfish que não clona direito o objeto.
      // Ao usar o ArrayList o Glassfish funciona melhor. Ou seja, para passar pela fachada o Arraylist é mais rápido e evita stackoverflow de "copyObject"
      final ArrayList<RFWVO> vos = new ArrayList<>();

      // Lista de listas de obejtos que precisam ser "limpas"
      // Quando as listas tem um sortIndex definido, o banco envia os objetos conforme o orderBy definido do banco e não como deveriamos ter na lista.
      // Como não temos o sortIndex salvo no VO, o valor será jogado fora com o ResultSet, nós criamos objetos "dummies" para ocupar a posição informada pelo indexOrder até que o objeto correto seja recuperado.
      // PROBLEMAS: se por algum motivo o sortIndex ficou errado, digamos faltando o elemento 4 de um total de 10, por exemplo pq o mapeamento foi apagado por ON DELETE CASCADE do banco,
      // a lista acaba sendo retornada com o dummie objetct ocupando a posição do item que sumiu. Por isso salvamos aqui todas as listas que tiveram dummies objetos colocados, para que no fim da montagem possamos limpar essas listas e manter a ordem desejada
      final HashSet<List<?>> cleanLists = new HashSet<List<?>>();

      // Cache com os objetos já criados, assim reaproveitamos ao invés de criar várias instâncias do mesmo objeto.
      final HashMap<String, RFWVO> objCache;
      if (cache == null) {
        objCache = new HashMap<>();
      } else {
        objCache = cache;
      }

      while (rs.next()) {
        final HashMap<String, RFWVO> aliasCache = new HashMap<>(); // este cache armazena os objetos desse ResultSet. Sendo que a chave da Hash é o Alias utilizado no SQL para representar o objeto/tabela.

        // Vamos iterar cada tabela para criar seus objetos principais
        for (DAOMapTable mTable : map.getMapTable()) {
          if (mTable.path.startsWith("@")) { // Tabelas de Collection (RFWMetaCollection)
            // Verifica se temos a coluna ID no resultSet, isso indica que a tabela do objeto foi recuperada
            boolean retrived = false;
            try {
              getRSLong(rs, mTable.schema, mTable.table, mTable.alias, mTable.column, dialect); // rs.getLong(mTable.alias + "." + mTable.column);
              // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
              // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
              retrived = true;
            } catch (RFWException e) {
              if (e.getCause() != null && e.getCause() instanceof SQLException) {
                // um SQLException indica que a coluna não está presente, provavelmente pq os objetos não foram exigidos. Neste caso pulamos este objeto.
              } else {
                throw e;
              }
            }

            if (retrived) {
              // Busca o objeto 'pai', que tem a collection, pelo ID definido na foreingKey
              DAOMapTable joinTable = map.getMapTableByAlias(mTable.joinAlias);
              Long parentID = getRSLong(rs, joinTable.schema, joinTable.table, joinTable.alias, "id", dialect); // Long parentID = rs.getLong(mTable.joinAlias + ".id");
              if (rs.wasNull()) parentID = null;

              // Se não temos um ID do Pai, não temos nem um objeto para incializar
              if (parentID != null) {
                final DAOMapTable parentTable = joinTable;
                final String key = parentTable.type.getCanonicalName() + "." + parentID;
                RFWVO vo = objCache.get(key);

                // Se temos a chave da FK mas não encontramos o objeto, temos um probema... Se não temos a FK é pq provavelmente já temos o objeto pai nulo também...
                if (vo == null) throw new RFWCriticalException("Não foi possível encontrar o pai para colocar os valores da RFWMetaCollection!", new String[] { mTable.table, mTable.column });

                // Recupera o field com o valor baseado no atributo do Path da Tabela. Note que o Path da tabela tem o @ no início para identificar que é uma tabela de MetaCollection. Já os campos, tem o @ no fim do nome do field, por isso a operação remover a @ do começo e concatena-la no final.
                final DAOMapField mField = map.getMapFieldByPath(mTable.path.substring(1) + "@");

                final RFWMetaCollectionField ann = (RFWMetaCollectionField) RUReflex.getRFWMetaAnnotation(vo.getClass(), mField.field.substring(0, mField.field.length() - 1));
                if (ann.targetRelationship() == null) throw new RFWCriticalException("Não foi possível encontrar o TargetRelationship da MetaCollection em '" + vo.getClass().getCanonicalName() + "' do método '" + mField.field.substring(0, mField.field.length() - 1) + "'.");

                // Recupera o conteúdo a ser colocado na MetaCollection
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
                    throw new RFWCriticalException("RFWDAO não preparado para tratar Collections com target do tipo '" + ann.targetRelationship().getCanonicalName() + "'!");
                  }
                } catch (SQLException e) {
                  // Uma SQLException ao tentar recuperar a coluna, indica que a coluna não foi "solicitada", assim não temos nada para adicionar, nem o objeto Hash
                }

                if (content != null) {
                  // Com o VO em mãos, verificamos o tipo de MetaCollection que temos na entidade (Map ou List) para saber como popular e instanciar se ainda for o primeiro
                  final Class<?> rt = RUReflex.getPropertyTypeByType(parentTable.type, mField.field.substring(0, mField.field.length() - 1)); // Remove a @ do final do Field
                  if (List.class.isAssignableFrom(rt)) {
                    // Se é um List procuramos a coluna de 'sort' para saber como organizar os itens
                    DAOMapField sortField = map.getMapFieldByPath(mTable.path.substring(1) + "@sortColumn");
                    Integer sortIndex = null; // A coluna de organização não é obrigatória para montar uma lista, só deixamos de garantir a ordem.
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
                        // Se não temos sortIndex, simplesmente adicionamos à lista a medida que vamos recuperando
                        list.add(content);
                      } else {
                        if (list.size() == 0) cleanLists.add(list); // Se é nova, e temos indexação, incluímos a lista para limpeza depois.
                        // Se temos um index, temos de repeita-lo e ir populando a lista corretamente. Como podemos receber a lista fora de ordem temos de verificar o tamanho da lista antes de inserir o objeto. Caso a lista ainda seja menor do que a posição a ser ocupada, incluímos alguns "Dummy" Objects para crescer a lista e já deixar o objeto na posição correta
                        while (list.size() < sortIndex + 1)
                          list.add(map); // incluimos um objeto que já existe para evitar instanciar novos objetos na memória. E Adicionar NULL não funciona na Linked List
                        // Incluímos inclusive um objeto no lugar onde nosso objeto será colocado propositalmente, assim não precisamos ter duas lógicas abaixo
                        list.add(sortIndex, content); // Ao incluir o objeto no índex determinado, empurramos todos os outros para frente, por isso temos de remover o próximo objeto (que antes ocupava o lugar deste)
                        list.remove(sortIndex + 1);
                      }
                      RUReflex.setPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1), list, false);
                    }
                  } else if (HashSet.class.isAssignableFrom(rt)) {
                    HashSet set = (HashSet) RUReflex.getPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1));
                    if (set == null) {
                      set = new HashSet<>();
                    }
                    set.add(content); // não testamos se já existe pq o SET não permite itens repetidos, ele substituirá automaticamente itens repetidos
                    RUReflex.setPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1), set, false);
                  } else if (Map.class.isAssignableFrom(rt)) {
                    // Se é um Map procuramos a coluna de 'key' para saber a chave que devemos incluir na Map
                    DAOMapField keyField = map.getMapFieldByPath(mTable.path.substring(1) + "@keyColumn");
                    Object keyValue = getRSString(rs, mTable.schema, mTable.table, mTable.alias, keyField.column, dialect); // rs.getString(mTable.alias + "." + keyField.column);

                    // Verifica a existência de um Converter para a chave de acesso
                    if (RFWDAOConverterInterface.class.isAssignableFrom(ann.keyConverterClass())) {
                      // Object ni = ann.keyConverterClass().newInstance();
                      Object ni = createNewInstance(ann.keyConverterClass());
                      if (!(ni instanceof RFWDAOConverterInterface)) throw new RFWCriticalException("A classe '${0}' definida no atributo '${1}' da classe '${2}' não é um RFWDAOConverterInterface válido!", new String[] { ann.keyConverterClass().getCanonicalName(), mField.field, vo.getClass().getCanonicalName() });
                      keyValue = ((RFWDAOConverterInterface) ni).toVO(keyValue);
                    }

                    Map hash = (Map) RUReflex.getPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1));
                    if (hash == null) {
                      hash = new LinkedHashMap<>();
                    }
                    // Recupera o atributo do objeto que é usado como chave da hash
                    if (!hash.containsKey(keyValue)) {
                      hash.put(keyValue, content); // Só adiciona se ainda não tiver este objeto
                      RUReflex.setPropertyValue(vo, mField.field.substring(0, mField.field.length() - 1), hash, false);
                    }
                  } else {
                    throw new RFWCriticalException("O tipo ${0} não é suportado pela RFWMetaCollection! Atributo '${1}' da classe '${2}'.", new String[] { rt.getCanonicalName(), mField.field.substring(0, mField.field.length() - 1), parentTable.type.getCanonicalName() });
                  }
                }
              }
            }
          } else if (mTable.path.startsWith(".")) { // Tabelas de N:N (join Tables)
            // Ignora as tabelas de N:N, não faz nada!
          } else {
            // Verifica se temos a coluna ID no resultSet, isso indica que a tabela do objeto foi recuperada
            Long id = null;
            boolean retrived = false;
            try {
              id = getRSLong(rs, mTable.schema, mTable.table, mTable.alias, "id", dialect); // rs.getLong(mTable.alias + ".id");
              // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
              // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
              retrived = true;
            } catch (RFWException e) {
              if (e.getCause() != null && e.getCause() instanceof SQLException) {
                // um SQLException indica que a coluna não está presente, provavelmente pq os objetos não foram exigidos. Neste caso pulamos este objeto.
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

                // Iteramos os campos em busca das informações deste VO
                for (DAOMapField mField : map.getMapField()) {
                  // Ignora o field ID pq ele não está no objeto sendo escrito (é herdado do pai) e o método write não o encontra. Sem contar que já foi escrito diretamente acima sem a necessidade de reflexão
                  if (mField.table == mTable && !"id".equals(mField.field)) writeToVO(mField, (VO) vo, rs);
                }
              }
              aliasCache.put(mTable.alias, vo);
              // Se é um objeto da tabela raiz, adicionamos também na lista que vamos retornar. Não podemos ter essa linha só quando criamos o objeto pq as vezes um objeto raiz é criado primeiro por ter sido apontado em um relacionamento cíclico. E embor ao objeto já tenha sido criado em outra iteração, ele deve ser adiconado na lista de objetos raiz só agora.
              if ("".equals(mTable.path) && !vos.contains(vo)) vos.add(vo);

            } else if (retrived) {
              // Se não tem objeto, mas o procuramos, vamos incluir o objeto "nulo" na hash de cache para que fique explicito de que o procuramos o objeto, só não existe nenhuma associação
              aliasCache.put(mTable.alias, null);
            }
          }
        }

        // Com todos os objetos criados, só precisamos defini-los para montar a hierarquia
        for (int iterationControl = 0; iterationControl < 2; iterationControl++) {
          // O Loop do iteration control foi criado para garantir que as hashs só serão montadas em uma segunda iteração, depois que os demais objetos já foram montados. Isso porquê a chave da hash possa ter propriedades aninhadas em seus subobjetos, e garantir que eles já foram montados:
          // - Iteração 0: monta todos os objetos e os "relaciona" nas propriedades diretas e Listas
          // - Iteração 1: monta as Hashs para que suas propriedaes aninhadas não estejam nulas.
          for (DAOMapTable mTable : map.getMapTable()) {
            if (mTable.joinAlias != null && !mTable.path.startsWith(".")) { // joinAlias != null -> ignora a tabela raiz, Todos os objetos raíz não precisem ser colocados dentro de outro objeto, mas colocamos no array de objetos que vamos retornar | !mTable.path.startsWith(".") -> ignora as tabelas de N:N que não tem um objeto
              RFWVO vo = aliasCache.get(mTable.alias);
              // VO pode ser nulo caso a associação não exista
              if (vo != null) {
                RFWVO join = aliasCache.get(mTable.joinAlias);
                if (join == null) {
                  // Join é nulo quando temos entre este objeto e o objeto de joinAlias uma tabela de N:N. Neste caso temos de pular esse objeto
                  final DAOMapTable tmp = map.getMapTableByAlias(mTable.joinAlias);
                  // Se for mesmo a tabela de Join, ela tem o caminho começando com ".". Se não for uma tabela de join não utilizamos o objeto pois pode ser só um caso de 2 Joins de objetos diferentes e estamos pulando 1
                  if (tmp.path.startsWith(".")) join = aliasCache.get(tmp.joinAlias);
                }
                final String relativePath = RUReflex.getLastPath(mTable.path); // pega o caminho em ralação ao objeto atual, não desde a raiz
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
                    if (!list.contains(vo)) { // Só adiciona se ainda não tiver este objeto
                      // Se é uma lista, verificamos se no atributo do VO temos a definição da coluna de 'sortColumn', para montar a lista na ordem correta.
                      final RFWMetaRelationshipField ann = join.getClass().getDeclaredField(relativePath).getAnnotation(RFWMetaRelationshipField.class);
                      Integer sortIndex = null;
                      if (ann != null && !"".equals(ann.sortColumn())) {
                        sortIndex = getRSInteger(rs, mTable.schema, mTable.table, mTable.alias, ann.sortColumn(), dialect); // rs.getInt(mTable.alias + "." + ann.sortColumn());
                      }
                      // Verificamos se é um caso de composição de árvore
                      if (ann.relationship() == RelationshipTypes.COMPOSITION_TREE) {
                        // Nos casos de composição de árvore vamos recber o primeiro objeto, mas não os objetos filhos da árvore completa. Isso pq não teriamos como fazer infinitos JOINS no SQL para garantir que todos os objetos seriam retornados.
                        // Nestes casos vamos resolicitar ao DAO este objeto de forma completa, incluindo seu próximo filho. Isso será feito de forma recursiva até que todos sejam recuperados.
                        // Para aproveitar o mesmo cache de objetos, chamamos um método específico para isso, que criará um SQL baseado no DAOMap que já temos deste objeto e passando o cache de objetos
                        vo = fullFillCompositoinTreeObject(map, map.getMapTableByAlias(mTable.joinAlias), vo, objCache);
                      }
                      if (sortIndex == null) {
                        // Se não temos sortIndex, simplesmente adicionamos à lista a medida que vamos recuperando
                        list.add(vo);
                      } else {
                        if (list.size() == 0) cleanLists.add(list); // Se é nova, e temos indexação, incluímos a lista para limpeza depois.
                        // Se temos um index, temos de repeita-lo e ir populando a lista corretamente. Como podemos receber a lista fora de ordem temos de verificar o tamanho da lista antes de inserir o objeto. Caso a lista ainda seja menor do que a posição a ser ocupada, incluímos alguns "Dummy" Objects para crescer a lista e já deixar o objeto na posição correta
                        while (list.size() <= sortIndex + 3)
                          list.add(map); // incluimos um objeto que já existe para evitar instanciar novos objetos na memória. E Adicionar NULL não funciona na Linked List
                        // Incluímos inclusive um objeto no lugar onde nosso objeto será colocado propositalmente, assim não precisamos ter duas lógicas abaixo
                        list.add(sortIndex, vo); // Ao incluir o objeto no índex determinado, empurramos todos os outros para frente, por isso temos de remover o próximo objeto (que antes ocupava o lugar deste)
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
                    // Recupera o atributo do objeto que é usado como chave da hash
                    final String keyMapAttributeName = join.getClass().getDeclaredField(relativePath).getAnnotation(RFWMetaRelationshipField.class).keyMap();
                    final Object key = RUReflex.getPropertyValue(vo, keyMapAttributeName);
                    if (!hash.containsKey(key)) {
                      hash.put(key, vo); // Só adiciona se ainda não tiver este objeto
                      RUReflex.setPropertyValue(join, relativePath, hash, false);
                    }
                  }
                } else {
                  throw new RFWCriticalException("O RFWDAO não sabe montar mapeamento do tipo '${0}', presente no '${1}'.", new String[] { rt.getCanonicalName(), join.getClass().getCanonicalName() });
                }
              } else {
                // Se o objeto da tabela não foi montado, verifica se a chave consta na Hash. Isso não há objeto para associar, mas que o objeto foi procurado. Nesse caso não temos objeto para adicionar, mas devemos criar a Lista/Hash Vazia se ela ainda não existir
                if (aliasCache.containsKey(mTable.alias)) {
                  RFWVO join = aliasCache.get(mTable.joinAlias);
                  if (join == null) {
                    // Join é nulo quando temos entre este objeto e o objeto de joinAlias uma tabela de N:N. Neste caso temos de pular esse objeto
                    final DAOMapTable tmp = map.getMapTableByAlias(mTable.joinAlias);
                    // Se for mesmo a tabela de Join, ela tem o caminho começando com ".". Se não for uma tabela de join não utilizamos o objeto pois pode ser só um caso de 2 Joins de objetos diferentes e estamos pulando 1
                    if (tmp.path.startsWith(".")) join = aliasCache.get(tmp.joinAlias);
                  }
                  if (join != null) {
                    // Se joinAlias continuar nulo, é pq mesmo o objeto pai não foi montado. Isso pode acontecer am casos de múltiplos LEFT JOIN e o relacionamento anterior também retornou nulo. Como ele é nulo não precisamos criar a lista vazia
                    final String relativePath = RUReflex.getLastPath(mTable.path); // pega o caminho em ralação ao objeto atual, não desde a raiz
                    final Class<?> rt = RUReflex.getPropertyTypeByType(join.getClass(), relativePath);
                    if (RFWVO.class.isAssignableFrom(rt)) {
                      // Não faz nada, só não deixa cair no else
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
                      throw new RFWCriticalException("O RFWDAO não sabe montar mapeamento do tipo '${0}', presente no '${1}'.", new String[] { rt.getCanonicalName(), join.getClass().getCanonicalName() });
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
          // Essa flag é passada para true quando não tivemos uma exception nas linhas acima. Isso quer dizer que a coluna existe no resultado, só retornou nulo. Isso quer dizer buscamos pelo objeto mas não existe a associação.
          // Neste caso temos de inicializar as listas do objeto, mesmo que vá vazia, para indicar que procuramos pelas associações mesmo qu não exista nenhuma. Já que enviar null indica que nem procuramos.
        } catch (SQLException e) {
          throw new RFWCriticalException("Falha ao obter o valor da coluna no banco de dados.", e);
        }
        break;
      case DerbyDB:
        try {
          // Note que o DerbyDB não dá suporte à recuperar o valor pelo nome da coluna quando o select utiliza o * ou algo tipo t0.*. Nesses casos tentamos procurar utilizando os metadados
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
    // Se não for a tabela raiz, temos de criar um subMap para conseguir prosseguir, se for, já estamos com ele pronto (provavelmente pq já estamos seguindo a árvore desse objeto
    if (!"".equals(startTable.path)) map = map.createSubMap(startTable);
    try (Connection conn = ds.getConnection(); PreparedStatement stmt = DAOMap.createSelectCompositionTreeStatement(conn, map, startTable, vo.getId(), null, null, null, null, null, dialect); ResultSet rs = stmt.executeQuery()) {
      // Removemos o objeto atual da Cache, ou ele não será remontado conforme os novos dados
      objCache.remove(vo.getClass().getCanonicalName() + "." + vo.getId());
      final List<RFWVO> list = mountVO(rs, map, objCache);
      if (list.size() > 1) {
        throw new RFWCriticalException("O RFWDAO montou mais de um objeto de CompositionTree para o mesmo ID! Alguma falha de modelo da tabelas ou configuração de chaves de banco.");
      } else if (list.size() == 1) {
        return list.get(0);
      }
    } catch (RFWException e) {
      throw e;
    } catch (Throwable e) {
      throw new RFWCriticalException("Falha ao executar a operação no banco de dados.", e);
    }
    throw new RFWCriticalException("O RFWDAO falhou em montar o CompositionTree para um objeto que acabara de ser recuperado do banco.");
  }

  /**
   * Este método recupera o conteúdo do ResultSet, e de acordo com o tipo do atributo no VO, faz a conversão e passa o valor.
   *
   * @param mField Descritor do Mapeamento do Campo
   * @param vo VO onde a informação será escrita
   * @param rs ResultSet com o conteúdo do banco de dados.
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
        if (!(ni instanceof RFWDAOConverterInterface)) throw new RFWCriticalException("A classe '${0}' definida no atributo '${1}' da classe '${2}' não é um RFWDAOConverterInterface válido!", new String[] { convAnn.converterClass().getCanonicalName(), mField.field, vo.getClass().getCanonicalName() });
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
            // Se estiver no desenvolvimento imprime a exception com a mensagem de recomendação para que tenha o Stack da chamada completa, mas deixa o código seguir normalmente
            new RFWWarningException("O RFW não recomenda utilizar o 'java.util.Date'. Verifique a implementação e substitua adequadamente por LocalDate, LocalTime ou LocalDateTime.").printStackTrace();
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
              throw new RFWCriticalException("Não foi possível identificar o objeto '" + obj.getClass().getCanonicalName() + "' recebido para o atributo '" + mField.field + "' do VO: '" + vo.getClass().getCanonicalName() + "'. Tabela: '" + mTable.table + "." + mField.column + "'.");
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
          // Não fazemos nada. Isso pq o caso em que esse objeto aparece é quando temos uma coluna de FK na tabela do objeto. Para inserir só o ID não vamos criar o objeto para dar preferência em mandar sempre um objeto mais leve. Caso o objeto tenha sido solicitado explicitamente, ele será montado durante leitura da sua propria tabela e colocado aqui.
          // Assim inclusive garantimos que não vamos sobrepor um objeto completo por um só com o ID
        } else if (List.class.isAssignableFrom(dataType)) {
          // Não fazemos nada. Isso pq o caso em que esse objeto aparece é quando temos uma coluna de FK na tabela do objeto. Para inserir só o ID não vamos criar o objeto para dar preferência em mandar sempre um objeto mais leve. Caso o objeto tenha sido solicitado explicitamente, ele será montado durante leitura da sua propria tabela e colocado aqui.
          // Assim inclusive garantimos que não vamos sobrepor um objeto completo por um só com o ID
        } else {
          throw new RFWCriticalException("O RFWDAO não escrever no VO dados do tipo '${0}'.", new String[] { dataType.getCanonicalName() });
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

    // Iteramos todos os atributos para mapea-los também
    if (attributes != null) for (String attribute : attributes) {
      loadEntityMap(type, map, RUReflex.getParentPath(attribute)); // Removemos o último bloco de attribute pq é um atributo, e queremos passar para o método somente o "caminho" até a entidade.
      loadEntityCollectionMap(type, map, attribute);
    }

    return map;
  }

  /**
   * Carrega o mapeamento de uma entidade (RFWVO) na estrutura de mapeamento do SQL.
   *
   * @param type Tipo da Entidade a ser carregada (Classe)
   * @param map Objeto de mapeamento onde os novos mapeamentos devem ser colocados.
   * @param path Caminho do atributo com o mapeamento para esta entidade. Passe "" para carregar como objeto raiz. NUNCA PASSE NULL, não testamos null para diminuir o if.
   * @throws RFWException
   */
  @SuppressWarnings("unchecked")
  private void loadEntityMap(Class<? extends RFWVO> root, DAOMap map, String path) throws RFWException {
    if (map.getMapTableByPath(path) == null) { // se já encontramos não precisamos processar o bloco abaixo, pois não só o mapeamento da tabela já foi criado, qualquer qualquer mapeamento de tabela N:N e seus campos
      // Variáveis com o prefixo "entity" referen-se à entidade sendo mapeada
      Class<? extends RFWVO> entityType = null;
      RFWDAOAnnotation entityDAOAnn = null;
      String entityTable = null;
      String entitySchema = null;
      String entityJoin = null;
      String entityColumn = null;
      String entityJoinColumn = null;
      DAOMapTable mapTable = null;

      if (path.equals("")) { // Deixado nessa ordem, ao invés do "".equals(path) para justamente dar nullpointer caso alguém passe null. Em caso de null o if retornria true e estragaria a lógica de qualquer forma. Como null não é esperado, conforme javadoc, é melhor que dê nullpointer logo aqui para que o real problema seja encontrado (que é de onde vem o null)
        entityType = getEntity(root); // Se estamos no objeto raiz, a entidade é exatamente o objeto raiz
        entityDAOAnn = entityType.getAnnotation(RFWDAOAnnotation.class);
        entityTable = getTable(entityType, entityDAOAnn);
        entitySchema = getSchema(entityType, entityDAOAnn);

        PreProcess.requiredNonNull(entitySchema, "O RFWDAO não conseguiu determinar o schema a ser utilizada com a entidade: '${0}'.", new String[] { entityType.getCanonicalName() });
        PreProcess.requiredNonNull(entityTable, "O RFWDAO não conseguiu determinar a tabela a ser utilizada com a entidade: '${0}'.", new String[] { entityType.getCanonicalName() });

        mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, entityColumn, entityJoin, entityJoinColumn);
      } else {
        String parentPath = RUReflex.getParentPath(path);
        if (parentPath != null) {
          // Se temos um caminho e obtivemos um caminho do pai, procuramos se essa tabela já foi mapeada
          DAOMapTable parentMapTable = map.getMapTableByPath(parentPath);
          if (parentMapTable == null) {
            // Se não encontramos, chamamos o método recursivamente para primeiro cadastrar o pai
            loadEntityMap(root, map, parentPath);
            // Recuperamos o parent recem criado na recursão
            parentMapTable = map.getMapTableByPath(parentPath);
          }

          // Se temos o pai, temos de verificar o relacionamento que entre o objeto pai e este e suas definições. Dependendo do tipo de relacionamento, posicionamento da FK e etc., o mapeamento será diferente
          Field parentField = null;
          try {
            parentField = parentMapTable.type.getDeclaredField(RUReflex.getCleanPath(RUReflex.getLastPath(path)));
          } catch (Exception e) {
            throw new RFWCriticalException("", new String[] { RUReflex.getCleanPath(RUReflex.getLastPath(path)), parentMapTable.type.getCanonicalName() }, e);
          }
          final RFWMetaRelationshipField parentRelAnn = parentField.getAnnotation(RFWMetaRelationshipField.class);

          // Para definir a entidade destino primeiro tentamos retirar do atributo da anotação da classe pai, se não tiver tentamos verificar o tipo do atributo. A ordem é usada assim pr conta de litas, hashs e interfaces que impedem a detecção automaticamente
          if (!parentRelAnn.targetRelationship().equals(RFWVO.class)) {
            entityType = parentRelAnn.targetRelationship();
          } else if (RFWVO.class.isAssignableFrom(parentField.getType())) {
            entityType = (Class<? extends RFWVO>) parentField.getType();
          } else {
            throw new RFWCriticalException("Não foi possível detectar a classe de relacionamento do atributo '${0}'. Verifique se é um RFWVO ou se a classe está definida corretamente no 'targetRelatioship'.", new String[] { parentMapTable.type.getCanonicalName() + "." + parentField.getName() });
          }

          entityType = getEntity(entityType);
          entityDAOAnn = entityType.getAnnotation(RFWDAOAnnotation.class);
          entitySchema = getSchema(entityType, entityDAOAnn);
          entityTable = getTable(entityType, entityDAOAnn);

          PreProcess.requiredNonNull(entitySchema, "O RFWDAO não conseguiu determinar o schema a ser utilizada com a entidade: '${0}'.", new String[] { entityType.getCanonicalName() });
          PreProcess.requiredNonNull(entityTable, "O RFWDAO não conseguiu determinar a tabela a ser utilizada com a entidade: '${0}'.", new String[] { entityType.getCanonicalName() });

          switch (parentRelAnn.relationship()) {
            case MANY_TO_MANY:
              // Se o relacionamento entre um objeto e outro é N:N temos de criar um mapeamento de tabela de "joinAlias", se ainda não existir
              DAOMapTable joinMapTable = map.getMapTableByPath("." + path); // Tabelas de Join tem o mesmo caminho da tabela da entidade relacionada, precedidas de um '.'
              if (joinMapTable == null) joinMapTable = map.createMapTable(null, "." + path, parentMapTable.schema, getMetaRelationJoinTable(parentField, parentRelAnn), getMetaRelationColumn(parentField, parentRelAnn), parentMapTable.alias, "id");
              // Usamos as informações do mapeamento de joinAlias criado para criar o mapeamento desta entidade agora
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, "id", joinMapTable.alias, getMetaRelationColumnMapped(parentField, parentRelAnn));
              break;
            case PARENT_ASSOCIATION:
              if ("".equals(getMetaRelationColumn(parentField, parentRelAnn))) throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' está marcado como PARENT_ASSOCIATION, mas não tem o atributo 'column' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, "id", parentMapTable.alias, getMetaRelationColumn(parentField, parentRelAnn));
              break;
            case WEAK_ASSOCIATION:
              // No mapeamento, este tipo de relacionamento se comporta igualzinho o ASSOCIATION, por isso deixamos seguir para o ASSOCIATION e realizar o mesmo tipo de mapeamento/regras.
            case ASSOCIATION:
              if (!"".equals(getMetaRelationColumn(parentField, parentRelAnn))) {
                // Se a coluna mapeada está na tabela pai
                mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, "id", parentMapTable.alias, getMetaRelationColumn(parentField, parentRelAnn));
              } else if (!"".equals(getMetaRelationColumnMapped(parentField, parentRelAnn))) {
                // Se a coluna mapeada está na nossa tabela
                mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, getMetaRelationColumnMapped(parentField, parentRelAnn), parentMapTable.alias, "id");
              } else {
                throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' está marcado como ASSOCIATION, mas não tem o atributo 'column' nem 'columMapper' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              }
              break;
            case COMPOSITION:
              // Se é composição, criamos o mapeamento considerando que a coluna de joinAlias está na tabela que estamos mapeando agora
              if ("".equals(getMetaRelationColumnMapped(parentField, parentRelAnn))) throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' está marcado como COMPOSITION, mas não tem o atributo 'columnMapped' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, getMetaRelationColumnMapped(parentField, parentRelAnn), parentMapTable.alias, "id");
              break;
            case COMPOSITION_TREE:
              // Se é composição de hierarquia, criamos o mapeamento só do primeiro objeto mas não vamos dar sequência recursivamente. A sequência recursiva deverá ser tratada dinamicamente no objeto posteriormente
              if ("".equals(getMetaRelationColumnMapped(parentField, parentRelAnn))) throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' está marcado como COMPOSITION_TREE, mas não tem o atributo 'columnMapped' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, getMetaRelationColumnMapped(parentField, parentRelAnn), parentMapTable.alias, "id");
              break;
            case INNER_ASSOCIATION:
              // Uma associação interna é similar a uma PARENT ASSOCIATION, mesmo que o RFWDAO vá reutilizar o objeto caso ele já exista, em casos de consultas específicas precisamos fazer o Join da tabela de qualquer forma.
              if ("".equals(getMetaRelationColumn(parentField, parentRelAnn))) throw new RFWCriticalException("O atributo '${0}' da entidade '${1}' está marcado como INNER_ASSOCIATION, mas não tem o atributo 'column' definido corretamente.", new String[] { parentField.getName(), parentMapTable.type.getCanonicalName() });
              mapTable = map.createMapTable(entityType, path, entitySchema, entityTable, "id", parentMapTable.alias, getMetaRelationColumn(parentField, parentRelAnn));
              break;
          }
        }
      }

      // mapeamos o atributo "id" que não é enxergado pq está na classe pai RFWVO.
      map.createMapField(path, "id", mapTable, "id"); // Este atributo segue o padrão que no banco de ser sempre "id" e no objeto herda sempre o atributo "id" da classe pai.
      // RFWDAO.dumpDAOMap(map)
      // Tendo o mapeamento da tabela feito, iteramos a classe para mapear todos os seus fields que tenham alguma anotação RFWMeta definida. Qualquer attributo sem uma annotation RFWMeta é ignorado.
      for (Field field : RUReflex.getDeclaredFieldsRecursively(entityType)) {
        final Annotation metaAnn = RUReflex.getRFWMetaAnnotation(field);
        if (metaAnn != null) {

          // Se não tivermos um nome de coluna definido, utilizaremos o nome do próprio atributo da classe
          String fieldColumn = field.getName();
          try {
            // Tentamos recuperar por reflexão o field "column" da annotation já que não sabemos qual annotation é. Se falhar não tem problema, deixa o código seguir e tentar usar o nome da coluna como sendo do atributo da classe.
            String t = (String) metaAnn.annotationType().getMethod("column").invoke(metaAnn);
            if (!"".equals(t)) fieldColumn = t; // Só passamos se não estiver "", já que "" é o valor padrão do atributo column na annotation.
          } catch (Throwable e) {
            throw new RFWCriticalException("Não encontramos o atributo 'column' na RFWMetaAnnotation '${0}'. Este atributo é obrigatório em todas as RFWMetaAnnotations.", new String[] { metaAnn.annotationType().getCanonicalName() });
          }

          if (metaAnn instanceof RFWMetaRelationshipField) {
            RFWMetaRelationshipField relAnn = (RFWMetaRelationshipField) metaAnn;
            if (relAnn.relationship() != RelationshipTypes.MANY_TO_MANY && !"".equals(getMetaRelationColumn(field, relAnn))) {
              // Se a coluna com a FK fica na tabela deste objeto (ou seja o column foi definido e não é um relacionamento N:N)
              map.createMapField(path, field.getName(), mapTable, getMetaRelationColumn(field, relAnn));
            } else {
              // Relacionamentos que não tenham a FK dentro da tabela do objeto em questão, não precisam ser mapeadas. Isso pq ou não serão recuperados de qualquer forma, ou serão mapeados depois no mapa da entidade associada.
            }
          } else if (metaAnn instanceof RFWMetaCollectionField) {
            // Se for um collection não cadastramos nada neste momento, os atirbutos de collection precisam ser solicitados explicitamente.
            // Ao recebermos o atributo do collection ele é tratado posteriormente no método loadEntityCollectionMap
          } else {
            // Se não for um Relationship, deixamos simplesmente cadastramos o mapeamento comum
            map.createMapField(path, field.getName(), mapTable, fieldColumn);
          }
        }
      }
    }
  }

  private void loadEntityCollectionMap(Class<? extends RFWVO> root, DAOMap map, String attribute) throws RFWException {
    if (!"id".equals(attribute)) {
      if (attribute.endsWith("@") && attribute.length() > 0) attribute = attribute.substring(0, attribute.length() - 1);

      // Verifica se será substituído
      Class<? extends RFWVO> entityType = getEntity(root);

      Annotation ann = RUReflex.getRFWMetaAnnotation(entityType, attribute);
      if (ann instanceof RFWMetaCollectionField) {
        RFWMetaCollectionField colAnn = (RFWMetaCollectionField) ann;
        String parentPath = RUReflex.getParentPath(attribute); // Recuperamos o caminho pai para obter o mapeamento do pai. Já devemos ter todos pois o método loadEntityMap deve ser sempre chamado antes deste método

        String[] paths = attribute.split("\\.");
        String fieldName = paths[paths.length - 1];

        // Validamos se já não associamos essa tabela (isso pode acontecer se o usuário solicitar mais de uma vez o mesmo atributo de collection... estupido...)
        if (map.getMapTableByPath("@" + RUReflex.addPath(parentPath, fieldName)) == null) {
          DAOMapTable daoMapTable = map.getMapTableByPath(parentPath);

          // para indicar que o caminho é uma Collection, o path recebe um '@' como prefixo do path
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
   * Este método permite que o {@link DAOResolver} substitua um objeto por outro a ser mapeado em seu lugar.
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
   * Retorna o schema/catalog conforme definição da annotation da entidade.
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
    // Verifica se foi pasado na construção do RFWDAO
    if (schema == null) {
      schema = this.schema;
    }
    // Verifica se temos na entidade
    if (schema == null) {
      schema = entityDAOAnn.schema();
    }
    if (schema == null) {
      throw new RFWCriticalException("Não há um schema definido para a entidade '" + entityType.getCanonicalName() + "'.");
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
   * Retorna a tabela conforme definição da annotation da entidade.
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
      throw new RFWCriticalException("Não há uma tabela definida para a entidade '" + entityType.getCanonicalName() + "'.");
    }
    return table;
  }

  /**
   * Método para DEBUG, retorna uma String (e imprime no console) o conteúdo do Map, tanto mapeamento das tabelas quanto dos campos.
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
