# Contest-specific assets

この競技だけで成立するものを集める。`tools/`の他のディレクトリは、ここを参照する側であっても
競技非依存に保つ。次の競技を始めるときは、このディレクトリの中身を入れ替えるか削除する。

ここへ置く基準は「前提が次の競技でも無条件に成り立つか」である。成り立たないもの、
採る解法次第でしか成り立たないものがここへ入る。固有名詞を含むかどうかでは判断しない。
汎用coreのアプリ固有adapterも、差し替え対象を一箇所へ集めるためここへ置く。

| ファイル | 読む側 | 内容 |
| --- | --- | --- |
| `bench-patterns.json` | `tools/bench-output` | ベンチ出力のscore・合否正規表現。coreは形式を持たない |
| `load-phase.yaml` | `tools/analysisctl` | 任意。ベンチログの実負荷開始・停止行の正規表現。`tools/analysis/sources.yaml`の`load_phase`で参照する |
| `user-transition-routes.json` | `tools/user-transition-metrics` | Cookie列と正規化routeの宣言 |
| `analysis-schema/` | `tools/analysis/sources.yaml` | ベンチログの解釈をDuckDBのviewにするschema。エラー行の意味ビューとscore集計 |
| `smoke/` | `task setup-smoke` | ベンチを使わず計測経路を確認する、当日のセッションフローのGo実装 |
| `deployments.yaml` | `tools/deployctl` | 汎用graphをincludeし、解法固有のdeploymentとplanを置き換えるoverlay。追加seedもここに置く |

templateに置いてあるのは`bench-patterns.json`、`user-transition-routes.json`、
`analysis-schema/bench-errors-semantic.sql`、`smoke/`のプレースホルダーだけである。setupで当日の形式へ書き換え、
必要になった解法固有の宣言だけをこのディレクトリへ足す。`Taskfile.contest.yml`のhookタスク
（`prepare-db-assets`、`gen-contest-routing`、`setup-contest-assets`、`setup-smoke-probes`）も同じ考え方で、該当する処理がなければ空のままにする。

`analysis-schema/`のviewは、dashboardとREADMEが名前と列で参照する契約である。書式に合わせて
本体を書き換えるときも、view名と列は変えない。書き換えなければ0行を返す。

## ISURIDEの参加者向けログ

`task q-sync`で保存済みRUNの`bench.log`全文と以下のビューを分析DBへ反映する。
各行には`run_id`・`source_artifact`・`line_number`があり、RUN間の比較と原文への照合ができる。
最終情報は負荷窓で除外しない。`reported_at`は報告時刻であり、売上やエラーの個々の発生時刻ではない。

| ビュー | 分析用カラム |
| --- | --- |
| `bench_final_regions` | `region_name`、`registered_users`、`active_users` |
| `bench_final_owners` | `owner_name`、`sales`、`chairs` |
| `bench_registrations` | `registrations_reputation`、`registrations_invitation`（累計） |
| `bench_chair_growth` | `owner_name`、`added_chairs`、`reason`、`elapsed_s`（売上条件による椅子増加イベント） |
| `bench_dissatisfaction` | `matching_pct`、`pickup_pct`、`travel_pct`（百分率）、`elapsed_s` |
| `bench_latest_dissatisfaction` | 上記不満率のRUNごとの最終報告行。途中終了でも「完走」とはみなさない |
| `bench_completion` | `passed`、`score`、`error_types`（種別→件数のMAP）、`reported_error_count` |
| `bench_error_types` | `error_type`、`reported_count`（完了時の種別集計を1種別1行に展開） |
| `bench_info_coverage` | `record_type`、`parsed`。不明なINFO書式と既知書式の解析失敗を検出 |

地域・オーナーの名前は引用符とescapeを解釈済み。数値はBIGINT、不満率はDOUBLEとして取得できる。
既知の情報行には`parsed`と`raw_line`も残し、不正・欠損を0として補わない。
明示的な`map[]`だけを0件のエラーとして扱い、不満率の母数は未出力のためNULL。
負荷窓がなくても最終情報を取得でき、経過秒はNULLになる。
既存の`bench_progress`は負荷窓内の指標を1指標1行で返す契約を維持する。
DEBUGの内部情報は`bench_log_lines`や`bench_error_logs.content`の原文として保持する。

```shell
task q -- "SELECT run_id, region_name, registered_users, active_users FROM bench_final_regions ORDER BY run_id, region_name"
task q -- "SELECT run_id, owner_name, sales, chairs FROM bench_final_owners ORDER BY run_id, owner_name"
task q -- "SELECT run_id, matching_pct, pickup_pct, travel_pct FROM bench_latest_dissatisfaction ORDER BY run_id"
task q -- "SELECT run_id, line_number, record_type, raw_line FROM bench_info_coverage WHERE NOT parsed"
```

## ここへ置けないもの

- Goのテストは対象パッケージと同じディレクトリに置く必要がある。競技固有のテストは
  `tools/analysisctl/<contest>_test.go`のようにファイル名で競技固有と分かるようにし、
  `tools/template/paths.txt`の`[contest]`へ加えて、adapterを差し替えるときに一緒に削除する。
  当日のベンチ出力を解析できたことの確認も、`tools/dashboard/server/<contest>_test.go`のように
  こちら側へ置く。汎用テストは、書式に依存しない経路（成果物の有無とJSON契約）だけを見る。
- `tools/analysis/sources.yaml`と`tools/deployctl/deployments.yaml`に残る参照・値 — 構造は汎用なので
  ファイルごとは分けられない。該当行に`# contest:`コメントを付け、
  `grep -rn '# contest:' tools/`で一覧できるようにする。
