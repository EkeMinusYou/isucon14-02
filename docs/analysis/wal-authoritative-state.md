# WALを永続化の正本にする（sat、案②）

出典は `docs/conversation/20261011-052356_01a12779-8958-7a10-8679-90adf6eec46f.md` の2026-10-11 05:40:34、案②。基準コードはmain `535fdf8273643afb24cd88d23a3b160a052e2f65`。satの起点 `baf2975` との差は会話docsのみ。他案の索引化、配車変更、売上差分集計、ロール分離、計測追加は含めない。

## 保存済みEvidence

最新finalized RUN `20261011-053536` は225,300点、PASS。`run.json` のartifact statusと実在を照合した。MySQL digest/status、service metricsはstatus=okで実在する。生アクセスログはmanifestではokだが実ファイルがなく、`task artifacts-run RUN=runs/20261011-053536` はその欠損を報告する。生ログに依存する再解析や成果物の再生成はしていない。

古い `runs/analysis.duckdb` を更新せず、`task q` から保存済みTSVを直接読んだ。負荷開始後30〜60秒、同じホスト・同じ集計方法でのCPU消費はMySQL 14.264秒、Go 18.299秒。MySQL全量を投影へ帰属できない。

```sql
WITH m AS (
  SELECT *,
    epoch(CAST(timestamp AS TIMESTAMPTZ))
      - epoch(TIMESTAMPTZ '2026-10-10T20:35:41.321834887Z') AS t,
    (elapsed_ms - lag(elapsed_ms) OVER (
      PARTITION BY service ORDER BY elapsed_ms)) / 1000.0 AS dt
  FROM read_csv('runs/20261011-053536/isucon-1-service-metrics.tsv',
                delim='\t', header=true)
)
SELECT service, min(t), max(t), sum(cpu_pct / 100 * dt) AS cpu_seconds
FROM m WHERE t BETWEEN 30 AND 60 AND available=1 GROUP BY service;
```

MySQL digestでは座標投影535回・82,173 affected rows・SQL実行時間4.415秒、状態投影538回・17,754 affected rows・2.447秒、距離投影535回・78,388 affected rows・2.135秒が記録されている。digestは収集期間全体の値で、上の30秒窓のCPUとは異なる母数。affected rowsはupsertのMySQL集計値であり、距離更新の件数と同一ではない。ACK更新、ライド・椅子・評価統計のupsertも残っている。投影をなくすことでこれらのSQLとGo側のバッチ構築を削減する仮説であり、得点効果は未測定。

## 実装

- 座標全履歴、距離after-image、椅子稼働状態、ライド配車・評価、状態遷移、評価統計、通知ACKをWALへ保存し、MySQLへ投影しない。
- 投影worker、flush処理、pending配列、32,768件のbacklog制限、投影sequenceを削除。履歴レコードはディスク上に保持し、投影待ちのRAM backlogは持たない。既存のメモリ内ライド・通知履歴は維持する。
- 起動時はSQLの初期状態・登録・ライド作成・使用済みクーポンを読み、同一世代WALを先頭から1レコードずつ適用する。SQLに確定済みのライドとMATCHINGは重複作成せず、復元後に既存の通知順序再構築を行う。
- `state_projection` テーブルは初期化世代だけを保持する。テーブル名を維持することで既存WALとDBの世代対応も維持する。`1-schema.sql`、既存 `init.sh`、`POST /api/initialize` で世代を更新し、以前のWALを破棄する。`task db-recreate` の既存schema→起動→initialize経路で同じ契約を使う。
- 登録・payment token・クーポン付与/消費・ライド作成と最初のMATCHINGのSQL確定、決済処理は維持する。ライド継続判定、完了履歴、決済のライド一覧、オーナー売上・距離・評価、認証、通知は既にメモリを読むため、負荷中の投影に依存しない。
- 既存の500msごとのWAL syncと通常終了時のsync/closeを維持。電源断対策は追加しない。

## ローカル確認と評価条件

`task inspect-hosts` の読み取り専用SSHで3台ともLinux/x86_64を確認。`task build` は `CGO_ENABLED=0 GOOS=linux GOARCH=amd64` で成功し、ELF x86-64の静的バイナリを確認した。

`task check-chair-distance` は使い捨てローカルMySQLで `go test -race -count=1 -v .` を実行し、全件PASS（最終検査17.068秒）。SQL初期座標＋WAL全座標からの距離再計算、登録、座標API、状態遷移、評価、初期化を確認した。通常再起動相当としてWALをcloseして新しいstateを構築し、稼働状態・配車・座標/距離・完了/評価・ACK・SSE cursorを確認した。完了ACK前の椅子再利用禁止、ACK後の再利用、二度の再起動でも統計が重複しないこと、古い世代のACK拒否、SQL投影なしで32,769件を受理して復元できることも確認した。

本変更の配布対象は単一Goのisucon-1とSQL初期化資材。WALは既存の `/home/isucon/state` を使う。デプロイ、サーバーのサービス操作、競技DB再作成、ベンチ実行は行わない。

最終評価はユーザーがsatから行う単独RUNのPASS・score、同一時間窓のMySQL/Go CPU秒とSQL書込量、座標/状態/評価API、通常再起動後の受理済みデータ取得で判断する。登録・ライド作成のSQL仕事は残る。再起動時は全WALを読むため履歴量に応じた復元時間が増え、DB単体には負荷中の最新状態が存在しなくなる。通常再起動後の取得にはDBと当該世代WALの両方が必要になる。

## main取り込み後

上記は案②の単独実装時の記録。その後ユーザーからmain取り込みとデプロイの指示を受け、main `251b308a24d8a0c227d39e31e2976c812575a4cf` をsatへマージした。mainで採用済みの現在状態索引、売上差分集計、配車・評価計測、3台ロール分離を含む。WAL全件再生後に通知・現在状態・売上の索引を再構築し、SQL投影は削除したまま維持する。売上復元テストを投影後の再起動からWALのみでの二度の再起動へ変更し、椅子再利用チェックはmainの配車可能集合を使う。

取り込み後の `task check-chair-distance` はrace付きで全件PASS（19.763秒）。`task deploy-all-dry` の対象確認とlinux/amd64向けbuildも成功した。読み取り専用SSHで実ホストの構成を確認し、`task check-roles` と `task check-network` は成功。既に1号機Go＋WAL＋決済mock、2号機nginx＋matcher、3号機MySQLの配置になっているため、今回の配布は既存 `task deploy-sql` と `task deploy-app` を使う。SQL初期化資材はGoホストだけでなくDB再作成用のMySQLホストにも必要であり、旧版資材のstampが残るとverifyも失敗するため、`sql_assets` roleを両ホストの重複を除いた集合にした。配布後は `task setup-postflight` で全ホストの配布物一致・role・疎通・計測停止を確認する。
