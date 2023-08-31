package org.apache.calcite.sql.parser;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.CharLiteralStyle;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.SchemaCustom;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;

import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;

import org.immutables.value.Value;

import java.io.StringReader;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.sql.parser.SqlParser.*;

public abstract class SqlParserPlus
{

  protected final SqlAbstractParserImpl parser;
  protected SqlParserPlus(SqlAbstractParserImpl parser) {
    this.parser = parser;
  }


    protected abstract Object getSchema();

  public abstract RexBuilder getRexBuilder();

  public abstract RelNode getOptimizedRelNode(SqlNode sqlNode);

  public abstract RelOptPlanner getPlanner();

  public abstract RelMetadataQuery getRelMetaDataQuery();



  protected abstract SqlNode parseX(String query) throws SqlParseException;


  public QueryExecutionNode parse(String sQueryId, String schemaName, String query)
  {
    try
    {
      SqlNode sqlNode = parseX(query);
      SqlKind sqlKind = sqlNode.getKind();
      if (!isDdl(sqlKind))
      {
        RelNode optimizedPhase2 = getOptimizedRelNode(sqlNode);
        return new QueryExecutionNode(sQueryId, schemaName, (SchemaCustom) getSchema(), query, optimizedPhase2, getRexBuilder(),
            getPlanner(), getRelMetaDataQuery());
      }
    }
    catch (SqlParseException ex)
    {
      throw new RuntimeException(ex.getMessage(), ex);
    }
    catch (UnsupportedOperationException ex)
    {
      String message = ex.getMessage() == null ? "Unsupported operation in sql" : ex.getMessage();
      throw new RuntimeException(message, ex);
    }
    catch(Throwable ex)
    {
      String message = ex.getMessage() == null ? "Error parsing query" : ex.getMessage();
      throw new RuntimeException(message, ex);
    }
    return null;
  }

  protected boolean isDdl(SqlKind sqlKind)
  {
    switch (sqlKind)
    {
    case SELECT:
    case INTERSECT:
    case VALUES:
    case WITH:
    case UNION:
    case EXCEPT:
    case INSERT:
    case DELETE:
    case UPDATE:
    case MERGE:
    case UNNEST:
    case OTHER_FUNCTION:
    case MULTISET_QUERY_CONSTRUCTOR:
    case MULTISET_VALUE_CONSTRUCTOR:
    case ORDER_BY:
      return false;
    default:
      return true;
    }
  }

  public static SqlParserPlus.Config config() {
    return SqlParserPlus.Config.DEFAULT;
  }

  public static SqlParserPlus create(String s, Config config) {
    return create(s, config());
  }


  @Deprecated // to be removed before 2.0
  public static SqlParserPlus.ConfigBuilder configBuilder() {
    return new SqlParserPlus.ConfigBuilder();
  }


  @Deprecated // to be removed before 2.0
  public static SqlParserPlus.ConfigBuilder configBuilder(SqlParserPlus.Config config) {
    return new SqlParserPlus.ConfigBuilder().setConfig(config);
  }


  public SqlNode parseQuery() throws SqlParseException {
    try {
      return parser.parseSqlStmtEof();
    } catch (Throwable ex) {
      throw handleException(ex);
    }
  }

  public SqlNode parseQuery(String sql) throws SqlParseException {
    parser.ReInit(new StringReader(sql));
    return parseQuery();
  }

  private SqlParseException handleException(Throwable ex) {
    if (ex instanceof CalciteContextException) {
      final String originalSql = parser.getOriginalSql();
      if (originalSql != null) {
        ((CalciteContextException) ex).setOriginalStatement(originalSql);
      }
    }
    return parser.normalizeException(ex);
  }




  @Value.Immutable
  @SuppressWarnings("deprecation")
  public interface Config {


    static final SqlParserPlus.Config INSTANCE = (Config) validate(String.valueOf(new ImmutableSqlParser.Config()));

    public static SqlParserPlus.Config of() {
      return INSTANCE;
    }

    static Object validate(String instance) {
      return INSTANCE != null && INSTANCE.equals(instance) ? INSTANCE : instance;
    }



    /** Default configuration. */
    SqlParserPlus.Config DEFAULT = SqlParserPlus.Config.of();

    @Value.Default default int identifierMaxLength() {
      return DEFAULT_IDENTIFIER_MAX_LENGTH;
    }

    SqlParserPlus.Config withIdentifierMaxLength(int identifierMaxLength);

    @Value.Default default Casing quotedCasing() {
      return Casing.UNCHANGED;
    }

    SqlParserPlus.Config withQuotedCasing(Casing casing);

    @Value.Default default Casing unquotedCasing() {
      return Casing.TO_UPPER;
    }

    SqlParserPlus.Config withUnquotedCasing(Casing casing);

    @Value.Default default Quoting quoting() {
      return Quoting.DOUBLE_QUOTE;
    }

    SqlParserPlus.Config withQuoting(Quoting quoting);

    @Value.Default default boolean caseSensitive() {
      return true;
    }

    SqlParserPlus.Config withCaseSensitive(boolean caseSensitive);

    @Value.Default default SqlConformance conformance() {
      return SqlConformanceEnum.DEFAULT;
    }

    SqlParserPlus.Config withConformance(SqlConformance conformance);

    @Deprecated // to be removed before 2.0
    @SuppressWarnings("deprecation")
    @Value.Default default boolean allowBangEqual() {
      return DEFAULT_ALLOW_BANG_EQUAL;
    }

    @Value.Default default Set<CharLiteralStyle> charLiteralStyles() {
      return ImmutableSet.of(CharLiteralStyle.STANDARD);
    }

    SqlParserPlus.Config withCharLiteralStyles(Iterable<CharLiteralStyle> charLiteralStyles);

    @Deprecated // to be removed before 2.0
    @Value.Default default Map<String, TimeUnit> timeUnitCodes() {
      return DEFAULT_IDENTIFIER_TIMEUNIT_MAP;
    }

    SqlParserPlus.Config withTimeUnitCodes(Map<String, ? extends TimeUnit> timeUnitCodes);

    @Value.Default default SqlParserImplFactory parserFactory() {
      return SqlParserImpl.FACTORY;
    }

    SqlParserPlus.Config withParserFactory(SqlParserImplFactory factory);

    default SqlParserPlus.Config withLex(Lex lex) {
      return withCaseSensitive(lex.caseSensitive)
          .withUnquotedCasing(lex.unquotedCasing)
          .withQuotedCasing(lex.quotedCasing)
          .withQuoting(lex.quoting)
          .withCharLiteralStyles(lex.charLiteralStyles);
    }
  }

  @Deprecated // to be removed before 2.0
  public static class ConfigBuilder {
    private SqlParserPlus.Config config = SqlParserPlus.Config.DEFAULT;

    private ConfigBuilder() {}

    /** Sets configuration to a given {@link SqlParser.Config}. */
    public SqlParserPlus.ConfigBuilder setConfig(SqlParserPlus.Config config) {
      this.config = config;
      return this;
    }

    public SqlParserPlus.ConfigBuilder setQuotedCasing(Casing quotedCasing) {
      return setConfig(config.withQuotedCasing(quotedCasing));
    }

    public SqlParserPlus.ConfigBuilder setUnquotedCasing(Casing unquotedCasing) {
      return setConfig(config.withUnquotedCasing(unquotedCasing));
    }

    public SqlParserPlus.ConfigBuilder setQuoting(Quoting quoting) {
      return setConfig(config.withQuoting(quoting));
    }

    public SqlParserPlus.ConfigBuilder setCaseSensitive(boolean caseSensitive) {
      return setConfig(config.withCaseSensitive(caseSensitive));
    }

    public SqlParserPlus.ConfigBuilder setIdentifierMaxLength(int identifierMaxLength) {
      return setConfig(config.withIdentifierMaxLength(identifierMaxLength));
    }

    public SqlParserPlus.ConfigBuilder setIdentifierTimeUnitMap(
        ImmutableMap<String, TimeUnit> identifierTimeUnitMap) {
      return setConfig(config.withTimeUnitCodes(identifierTimeUnitMap));
    }

    @SuppressWarnings("unused")
    @Deprecated // to be removed before 2.0
    public SqlParserPlus.ConfigBuilder setAllowBangEqual(final boolean allowBangEqual) {
      if (allowBangEqual != config.conformance().isBangEqualAllowed()) {
        return setConformance(
            new SqlDelegatingConformance(config.conformance()) {
              @Override public boolean isBangEqualAllowed() {
                return allowBangEqual;
              }
            });
      }
      return this;
    }

    public SqlParserPlus.ConfigBuilder setConformance(SqlConformance conformance) {
      return setConfig(config.withConformance(conformance));
    }

    public SqlParserPlus.ConfigBuilder setCharLiteralStyles(
        Set<CharLiteralStyle> charLiteralStyles) {
      return setConfig(config.withCharLiteralStyles(charLiteralStyles));
    }

    public SqlParserPlus.ConfigBuilder setParserFactory(SqlParserImplFactory factory) {
      return setConfig(config.withParserFactory(factory));
    }

    public SqlParserPlus.ConfigBuilder setLex(Lex lex) {
      return setConfig(config.withLex(lex));
    }

    public SqlParserPlus.Config build() {
      return config;
    }
  }
}
