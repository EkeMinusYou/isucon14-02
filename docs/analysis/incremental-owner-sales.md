# 売上の差分集計と期間検索

`main 885e40e`を起点にtueでローカル実装した。
出典は[相談ログの2026-10-11 05:22:04の追加案2](../conversation/20261011-050218_01a12767-7432-7433-9aa8-a835e30925f3.md)と
[得点構造の分析](benchmark-score-structure.md)。対象はGoの売上集計のみ。
配布する場合の対象はTaskfileのAPP_HOSTS `isucon-1`、サービスは`isuride-go`。
今回は本番deploy、DB再作成、service操作、競技ベンチ、mainへのマージを行っていない。

## 仮説と実装

変更前の`ownerGetSales`は、状態の読取りロックを保持したまま、対象椅子の
`chairRides`全履歴を毎回走査し、最新statusと時刻を判定して運賃を再計算する。
履歴成長後の再集計と、同じロックを使う座標更新への待ちを減らすことが目的。
売上API・状態ロック保持の短縮→座標更新待ちの減少／椅子増加の促進→スコア改善を期待する。
現時点でこの走査やロック待ちが律速という計測根拠はなく、効果量は未計測。

- 椅子別に、ライドの`UpdatedAt`順の累積売上を保持する。同じ時刻は1点に合算する。
- 通常の完了は末尾への追加で償却O(1)。最後と同時刻なら最後の累積値だけを更新する。
  非順序の完了は二分探索で挿入位置を求め、後続累積値を補正する（その椅子の履歴に対してO(N)）。
- 期間検索は2つの累積値の差で、椅子ごとにO(log N)。両端が全履歴を覆う場合はO(1)。
  owner・model集計には従来どおり椅子一覧を使うため、全椅子数に比例する走査は残る。
- ライド行とstatusの更新前後の売上寄与を比較し、変更分だけを反映する。
  同じ完了recordの再適用や評価のみの変更は増額しない。時刻・運賃の変更は旧寄与を引いて更新する。
- 通常更新は既存の`commit`→WAL書込み成功→`apply`の中で状態と売上を同時に公開する。
  書込み失敗では売上も公開しない。WAL形式・SQL schema・永続化方式は変更していない。
- 起動・initializeではSQLと未反映WALを読み、既存の`rebuildIndexes`の時刻順ソートを使って
  最終状態から累積値を再構築する。復元途中の売上更新は省く。

増えるメモリは椅子ごとのslice/mapと時刻別累積点。amd64では1点が32 bytes
（`time.Time` 24 bytes + `int` 8 bytes）、別途sliceの余剰capacityがある。
時刻を訂正した場合の寄与0の点も保持する。ライドごとの追加フィールドはない。
通常完了の運賃計算・累積更新と、非順序時の挿入／後続補正が書込みロック内に増える。
座標更新自体には売上の仕事を追加しない。新しいカウンター・ログ・計測基盤は追加していない。

## 維持する結果

- 売上対象は椅子割当済みで、最新statusが`COMPLETED`のライド。
  評価値があるだけの未完了ライドは対象外。
- 売上は`calculateSale`の割引前運賃。クーポン適用後の`memoryRide.Fare`は使用しない。
- 期間の時刻はstatusの`CreatedAt`ではなくライドの`UpdatedAt`。
  `since`は含み、`until`は指定時刻+999µsまで含む。nsを切り捨てて範囲を広げない。
  省略時の下限Unix epoch・上限9999年末と、不正なパラメータの400応答は維持する。
- ownerの椅子だけを集計し、同一modelを合算する。売上0の椅子・modelも返す。
  椅子のないownerは従来どおり`chairs: null`、`models: []`、`total_sales: 0`。
- 通常のサービス再起動後も、受理済み完了の全期間・期間指定の売上を取得できる。

## 変更前の保存RUN

既存`task q-sync`で分析DBを同期し、`task q`で以下を取得した。
この同期は保存済み成果物を分析DBへ取り込むだけで、RUN成果物は再生成していない。
最新finalizedは[20261011-052401](../../runs/20261011-052401/run.json)。
manifestと参加者結果の両方がPASS・185,859点。
計測したcommitは`3ab44a8c1a164ec3a0e81fcddf8e49aa79996743`（thu）。

| 指標 | 20261011-051806 | 20261011-052401 |
| --- | ---: | ---: |
| スコア／判定 | 45,032／PASS | 185,859／PASS |
| 売上API件数（全件2xx） | 163 | 164 |
| 売上API応答時間の合計 | 71.940s | 6.035s |
| 売上API平均／p90／p99 | 441／1,080／1,537ms | 37／67／253ms |
| 座標API件数（全件2xx） | 22,975 | 93,815 |
| 座標API応答時間の合計 | 2,068.148s | 5,229.568s |
| 座標API平均／p90／p99 | 90／187／337ms | 56／145／389ms |
| 初回椅子増加 | 開始後7.494s | 開始後7.423s |
| 椅子追加台数／最終台数 | 174／194 | 477／497 |

API件数・時間は`endpoints`（alp.json）のアクセスログ取得全体の集計。
複数リクエストの時間が重なるため、応答時間合計は壁時計時間やロック保持時間ではない。
椅子増加は`bench_chair_growth`、最終台数は`bench_final_owners`から取得した。
052401の`in_load_window`内でも追加477台・初回7.423sだった。
`load_windows`の052401の窓はJST 05:24:04.876332〜05:25:13.917890、69.041s。
これは参加者ログの負荷走行窓であり、公式採点区間と同一とは断定しない。
052401の評価API成功は2,736件（完了数の代理指標）。
上記2 RUNは実装・負荷条件が異なり、今回の売上差分集計の効果を示す比較ではない。

`artifacts`では052401の`alp.json`・`bench.log`・`user-transitions.json`は`ok`。
全artifactのstatusも`ok`。圧縮アクセスログはmanifest上`ok`／quality `valid`だが、
このworktreeには実ファイルがない。
`task artifacts-run RUN=runs/20261011-052401`は
`raw/access-*.log.zst`とcapture contractの`raw/access-isucon-1.log.zst`をmissingとして失敗した。
`run.json`にトップレベルの`missing`フィールドはない。欠損を0件に補わず、
取得済みの集計結果のみ使った。生ログから同一窓のAPIを再集計することはできない。

主な確認クエリ（`task q -- "SQL"`で実行）：

```sql
SELECT run_id, score, passed, commit, load_duration_ms, load_window_status
FROM manifests WHERE run_id >= '20261011-051806' ORDER BY run_id;

SELECT name, status, reason, quality_status
FROM artifacts WHERE run_id = '20261011-052401';

SELECT run_id, method, uri, count, status_2xx, sum_time_sec,
       avg_time_sec * 1000 AS avg_ms, p90_time_sec * 1000 AS p90_ms,
       p99_time_sec * 1000 AS p99_ms
FROM endpoints
WHERE run_id IN ('20261011-051806', '20261011-052401')
  AND (uri LIKE '%owner/sales%' OR uri LIKE '%chair/coordinate%');

SELECT run_id, min(elapsed_s) AS first_growth_s, sum(added_chairs) AS added_chairs
FROM bench_chair_growth WHERE run_id = '20261011-052401' AND in_load_window
GROUP BY run_id;

SELECT run_id, sum(chairs) AS final_chairs FROM bench_final_owners
WHERE run_id = '20261011-052401' GROUP BY run_id;
```

## ローカル検証と次の比較条件

- `task test-app`：Goアプリ全体のrace検査を含むテスト成功。
- `task check-chair-distance`：一時MySQLで公式初期化と全Goテスト／race検査に成功。
  新しい売上テストは割引付きSQL初期データ、未反映WALからの通常再起動、
  SQL反映後の再起動、initialize相当のデータ・generation変更後の復元を確認する。
- `task build`：既存の実ホスト確認（Linux x86_64）に合わせたlinux/amd64 build成功。
- `git diff --check`：成功。

期間テストは旧履歴走査を比較対象に、同時刻・非順序完了、境界の999µsとその1ns後、
逆転期間、片方／両方の指定省略を確認する。owner・model・0売上・空結果、
重複record・再評価・時刻／運賃訂正・WAL失敗時の非公開も確認した。

今回の仮説だけを変える同条件のRUNで、正当性PASSと期間／復元結果の一致を前提に、
売上APIの件数・平均・p90・p99・合計時間、座標APIの同指標、評価API成功件数、
椅子増加時刻・追加台数・最終台数、スコアを比較する。
完了更新の仕事が増えるため、評価API時間、既存Go profilesと資源指標、メモリも併せて見る。
APIの応答時間を状態ロック待ちと同一視せず、同一RUN・時間窓・母数が揃う指標で判断する。
配車イベント起動など別仮説との同時変更を避ける。ロック保持短縮と得点への寄与は未計測。
