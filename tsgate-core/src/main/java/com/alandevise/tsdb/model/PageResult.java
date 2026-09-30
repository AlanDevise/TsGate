package com.alandevise.tsdb.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Typed paginated result for annotated POJO queries.
 * <p>Supports both time-cursor and traditional limit/offset pagination. Cursor pagination uses
 * {@link #getNextCursorTime()} or {@link #getNextCursor()}, while offset pagination uses
 * {@link #getPageNum()} and {@link #getOffset()}.</p>
 *
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-07-02
 */
public class PageResult<T> {

    private final List<T> rows;
    private final Long nextCursorTime;
    private final Map<String, Object> nextCursor;
    private final boolean hasNext;
    private final int limit;
    private final SortOrderEnum order;
    private final Integer pageNum;
    private final Integer pageSize;
    private final Integer offset;
    private final Long total;
    private final Long totalPages;

    /**
     * Creates a time-cursor page result.
     *
     * @param rows           current page rows, for example {@code List.of(result1, result2)}
     * @param nextCursorTime next page's time cursor, for example {@code 1783000000000L}
     * @param hasNext        whether another page exists, for example {@code true}
     * @param limit          requested page size, for example {@code 50}
     * @param order          time sort direction, for example {@code SortOrderEnum.DESC}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public PageResult(List<T> rows, Long nextCursorTime, boolean hasNext, int limit, SortOrderEnum order) {
        this(rows, nextCursorTime, buildTimeCursor(nextCursorTime), hasNext, limit, order,
                null, null, null, null, null);
    }

    /**
     * Creates a composite-cursor page result.
     *
     * @param rows       current page rows, for example {@code List.of(result1, result2)}
     * @param nextCursor next page's composite cursor, for example {@code Map.of("time", 1783000000000L, "device_code", "D001")}
     * @param hasNext    whether another page exists, for example {@code true}
     * @param limit      requested page size, for example {@code 50}
     * @param order      time sort direction, for example {@code SortOrderEnum.ASC}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public PageResult(List<T> rows,
                      Map<String, Object> nextCursor,
                      boolean hasNext,
                      int limit,
                      SortOrderEnum order) {
        this(rows, extractCursorTime(nextCursor), nextCursor, hasNext, limit, order,
                null, null, null, null, null);
    }

    /**
     * Creates a traditional limit/offset page result.
     * <p>Retained for existing callers. Since no total count is supplied, {@link #getTotal()}
     * and {@link #getTotalPages()} return {@code null}. The template uses the constructor that accepts a total count.</p>
     *
     * @param rows     current page rows, for example {@code List.of(result1, result2)}
     * @param hasNext  whether another page exists, for example {@code true}
     * @param pageNum  current page number, for example {@code 2}
     * @param pageSize rows per page, for example {@code 50}
     * @param offset   current offset, for example {@code 50}
     * @param order    time sort direction, for example {@code SortOrderEnum.DESC}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public PageResult(List<T> rows,
                      boolean hasNext,
                      int pageNum,
                      int pageSize,
                      int offset,
                      SortOrderEnum order) {
        this(rows, null, null, hasNext, pageSize, order, pageNum, pageSize, offset,
                null, null);
    }

    /**
     * Creates a traditional limit/offset page result with a total count.
     *
     * @param rows     current page rows, for example {@code List.of(result1, result2)}
     * @param hasNext  whether another page exists, for example {@code true}
     * @param pageNum  current page number, for example {@code 2}
     * @param pageSize rows per page, for example {@code 50}
     * @param offset   current offset, for example {@code 50}
     * @param order    time sort direction, for example {@code SortOrderEnum.DESC}
     * @param total    total result rows matching the query, for example {@code 125L}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    public PageResult(List<T> rows,
                      boolean hasNext,
                      int pageNum,
                      int pageSize,
                      int offset,
                      SortOrderEnum order,
                      long total) {
        this(rows, null, null, hasNext, pageSize, order, pageNum, pageSize, offset,
                total, calculateTotalPages(total, pageSize));
    }

    /**
     * Common constructor that normalizes null collections and sort directions.
     *
     * @param rows           current page rows, for example {@code List.of(result1, result2)}
     * @param nextCursorTime next page's time cursor, for example {@code 1783000000000L}
     * @param nextCursor     next page's composite cursor, for example {@code Map.of("time", 1783000000000L)}
     * @param hasNext        whether another page exists, for example {@code true}
     * @param limit          requested page size, for example {@code 50}
     * @param order          time sort direction, for example {@code SortOrderEnum.DESC}
     * @param pageNum        offset page number, for example {@code 2}
     * @param pageSize       offset page size, for example {@code 50}
     * @param offset         pagination offset, for example {@code 50}
     * @param total          total rows for offset pagination, or {@code null} for cursor pagination
     * @param totalPages     total pages for offset pagination, or {@code null} for cursor pagination
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    private PageResult(List<T> rows,
                       Long nextCursorTime,
                       Map<String, Object> nextCursor,
                       boolean hasNext,
                       int limit,
                       SortOrderEnum order,
                       Integer pageNum,
                       Integer pageSize,
                       Integer offset,
                       Long total,
                       Long totalPages) {
        this.rows = rows == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(rows));
        this.nextCursorTime = nextCursorTime;
        this.nextCursor = immutableCursor(nextCursor);
        this.hasNext = hasNext;
        this.limit = limit;
        this.order = SortOrderEnum.normalize(order);
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.offset = offset;
        this.total = total;
        this.totalPages = totalPages;
    }

    /**
     * Returns the current page's rows.
     *
     * @return current page rows, for example {@code List.of(result1, result2)}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public List<T> getRows() {
        return rows;
    }

    /**
     * Returns the next page's time cursor.
     * <p>Omit the cursor on the first request. When {@link #isHasNext()} is {@code true},
     * pass this value unchanged to the next query's {@code cursorTime} parameter to retrieve the following page.
     * Returns {@code null} when no next page exists.</p>
     *
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-03
     */
    public Long getNextCursorTime() {
        return nextCursorTime;
    }

    /**
     * Returns the next page's composite cursor.
     * <p>Omit the cursor on the first request. When {@link #isHasNext()} is {@code true},
     * pass this map unchanged to the next query's {@code cursor(...)} parameter to retrieve the following page.
     * Returns an empty map when no next page exists.</p>
     *
     * @return next page's composite cursor, for example {@code Map.of("time", 1783000000000L, "device_code", "D001")}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    public Map<String, Object> getNextCursor() {
        return nextCursor;
    }

    /**
     * Determines whether another page exists.
     *
     * @return whether another page exists, for example {@code true}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public boolean isHasNext() {
        return hasNext;
    }

    /**
     * Returns the requested page size.
     *
     * @return requested page size, for example {@code 50}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Returns the time sort direction.
     *
     * @return time sort direction, for example {@code SortOrderEnum.DESC}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public SortOrderEnum getOrder() {
        return order;
    }

    /**
     * Returns the offset page number.
     *
     * @return offset page number, for example {@code 2}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public Integer getPageNum() {
        return pageNum;
    }

    /**
     * Returns the offset page size.
     *
     * @return offset page size, for example {@code 50}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public Integer getPageSize() {
        return pageSize;
    }

    /**
     * Returns the pagination offset.
     *
     * @return pagination offset, for example {@code 50}
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-02
     */
    public Integer getOffset() {
        return offset;
    }

    /**
     * Returns the total result count for offset pagination.
     * <p>Detail queries count matching detail rows; aggregate or grouped queries count aggregate result rows.
     * Cursor pagination does not run a count query and therefore returns {@code null}.</p>
     *
     * @return total result rows, for example {@code 125L}, or {@code null} for cursor pagination
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    public Long getTotal() {
        return total;
    }

    /**
     * Returns the total page count for offset pagination.
     *
     * @return total pages, for example {@code 3L}, or {@code null} for cursor pagination
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-25
     */
    public Long getTotalPages() {
        return totalPages;
    }

    /**
     * Calculates the total pages without overflowing during addition.
     *
     * @param total    total result rows
     * @param pageSize rows per page
     * @return total pages
     */
    private static long calculateTotalPages(long total, int pageSize) {
        if (total <= 0L) {
            return 0L;
        }
        return total / pageSize + (total % pageSize == 0L ? 0L : 1L);
    }

    /**
     * Builds a compatible composite cursor map from a single time cursor.
     *
     * @param nextCursorTime next page's time cursor, for example {@code 1783000000000L}
     * @return composite cursor map, or an immutable empty map when no next page exists
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static Map<String, Object> buildTimeCursor(Long nextCursorTime) {
        if (nextCursorTime == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> cursor = new LinkedHashMap<>();
        cursor.put("time", nextCursorTime);
        return cursor;
    }

    /**
     * Extracts time from a composite cursor for compatibility with the legacy nextCursorTime field.
     *
     * @param cursor composite cursor, for example {@code Map.of("time", 1783000000000L)}
     * @return epoch milliseconds, or null when unavailable
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static Long extractCursorTime(Map<String, Object> cursor) {
        if (cursor == null || cursor.isEmpty()) {
            return null;
        }
        Object value = cursor.get("time");
        if (value == null) {
            value = cursor.get("Time");
        }
        if (value == null) {
            value = cursor.get("_time");
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return null;
    }

    /**
     * Copies a composite cursor to prevent callers from mutating page-result state.
     *
     * @param cursor original composite cursor, for example {@code Map.of("time", 1783000000000L)}
     * @return an immutable cursor map, empty when no next page exists
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-07-07
     */
    private static Map<String, Object> immutableCursor(Map<String, Object> cursor) {
        if (cursor == null || cursor.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(cursor));
    }
}
