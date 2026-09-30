package com.alandevise.tsdb.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * WHERE condition in the common query model.
 * <p>The builder constructs this object and the adapter translates it into a backend SQL predicate.</p>
 *
 * @param column   filter column name
 * @param operator filter operator
 * @param values   filter values
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public record QueryFilter(String column,
                          OperatorEnum operator,
                          List<Object> values) {

    /**
     * Creates an immutable filter condition.
     *
     * @param column   filter column name, for example {@code "device_code"}
     * @param operator filter operator, for example {@code OperatorEnum.EQ}
     * @param values   filter values, for example {@code List.of("device001")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public QueryFilter(String column,
                       OperatorEnum operator,
                       List<Object> values) {
        this.column = column;
        this.operator = operator;
        this.values = values == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(values));
    }

}
