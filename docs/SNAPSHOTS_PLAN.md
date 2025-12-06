PRQL Integration Snapshot Coverage
==================================

Reference snapshots live in `tmp/prql/prqlc/prqlc/tests/integration/queries`. This table tracks which fixtures already have Go tests in `compile_test.go` (under `TestCompileSnapshots`) and which ones we still need to port.

| Snapshot file | Status | Notes / matching Go test |
| --- | --- | --- |
| `aggregation.prql` | Done | `TestCompileSnapshots/aggregation` |
| `append_select_compute.prql` | Done | `.../append_select_compute` |
| `append_select_multiple_with_null.prql` | Done | `.../append_select_multiple_with_null` |
| `append_select_nulls.prql` | Done | `.../append_select_nulls` |
| `append_select_simple.prql` | Done | `.../append_select_simple` |
| `append_select.prql` | Done | `.../append_select_union` |
| `arithmetic.prql` | Done | `.../arithmetic_div_mod` |
| `cast.prql` | Done | `.../cast_projection` |
| `constants_only.prql` | Done | `.../constants_only` |
| `date_to_text.prql` | Done | `.../date_to_text_formats` |
| `distinct_on.prql` | Done | `.../distinct_on_group_sort_take` |
| `distinct.prql` | Done | `.../distinct_group_take_one` |
| `genre_counts.prql` | Done | `.../genre_counts` |
| `group_all.prql` | Done | `.../group_all_join_aggregate` |
| `group_sort_derive_select_join.prql` | Done | `.../group_sort_derive_select_join` |
| `group_sort_filter_derive_select_join.prql` | Done | `.../group_sort_filter_derive_select_join` |
| `group_sort_limit_take.prql` | Done | `.../group_sort_limit_take_join` |
| `group_sort.prql` | Done | `.../group_sort_basic` |
| `invoice_totals.prql` | Done | `.../invoice_totals_window_join` |
| `loop_01.prql` | Done | `.../loop_recursive_numbers` |
| `math_module.prql` | Done | `.../stdlib_math_module` |
| `pipelines.prql` | Done | `.../pipelines_filters_sort_take` |
| `read_csv.prql` | Done | `.../read_csv_sort` |
| `set_ops_remove.prql` | Done | `.../set_ops_remove` |
| `sort_2.prql` | Done | `.../sort_alias_filter_join` |
| `sort_3.prql` | Done | `.../sort_alias_inline_sources` |
| `sort.prql` | Done | `.../sort_with_join_alias` |
| `switch.prql` | Done | `.../switch_case_display` |
| `take.prql` | Done | `.../take_range_with_sort` |
| `text_module.prql` | Done | `.../text_module_filters` |
| `window.prql` | Done | `.../window_functions` |

Summary
-------
- **Done:** 31 / 31 fixtures (matching upstream snapshots).
- **Partial:** None.
- **TODO:** None for the snapshot suite.
