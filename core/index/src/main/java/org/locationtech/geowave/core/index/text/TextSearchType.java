package org.locationtech.geowave.core.index.text;

import java.util.function.BiPredicate;

public enum TextSearchType {
  // for all but "contains" the Sort Keys of the query ranges should fully match expected search
  // results without the need for additional filtering via an "evaluate" BiPredicate
  EXACT_MATCH(TextIndexType.FORWARD, (value, term) -> (value != null) && value.equals(term)),
  BEGINS_WITH(TextIndexType.FORWARD),
  ENDS_WITH(TextIndexType.REVERSE),
  CONTAINS(TextIndexType.NGRAM, (value, term) -> (value != null) && value.contains(term));

  private TextIndexType indexType;
  private BiPredicate<String, String> evaluate;
  private boolean requiresEvaluate;

  private TextSearchType(final TextIndexType indexType) {
    this(indexType, TextIndexUtils.ALWAYS_TRUE, false);
  }

  private TextSearchType(
      final TextIndexType indexType,
      final BiPredicate<String, String> evaluate) {
    this(indexType, evaluate, true);
  }

  private TextSearchType(
      final TextIndexType indexType,
      final BiPredicate<String, String> evaluate,
      final boolean requiresEvaluate) {
    this.indexType = indexType;
    this.evaluate = evaluate;
    this.requiresEvaluate = requiresEvaluate;
  }

  public boolean evaluate(final String value, final String searchTerm) {
    return evaluate.test(value, searchTerm);
  }

  public boolean requiresEvaluate() {
    return requiresEvaluate;
  }

  public TextIndexType getIndexType() {
    return indexType;
  }
}
