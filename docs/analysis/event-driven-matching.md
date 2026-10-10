# 配車のイベント起動

イベント配車と200msへの限定修正は、下記の比較結果に基づきローカル実装から撤去した。以下は実験の経緯を残す記録であり、現行の実装・デプロイ手順ではない。

2026-10-11、`mon`、`main 885e40e`起点のローカル実装。担当は配車起動のみ。
売上集計、Hungarianの目的関数、候補選定、バッチ上限64件は変更しない。
**スコアへの効果は未計測。** 本番反映・ベンチ・DB再作成・サービス操作・mainへの取り込みは実施していない。

## 出典と現行Evidence

- [相談の2026-10-11 05:22:04の追加案1](../conversation/20261011-050218_01a12767-7432-7433-9aa8-a835e30925f3.md)：ライド作成と椅子の再利用可能確定から配車workerを起こす。
- [得点構造](benchmark-score-structure.md)：配車待ち削減 → 迎車・乗車開始の前倒し → 移動・評価・次ライドの増加 → 得点増。
- [公式マニュアル](../official/ISURIDE.md#ライドのマッチング)：内部matching APIとmatcherサービスの利用方法・仕様は変更可能。
- [正当性要件](../official/manual.md#負荷走行の打ち切り)：完了通知前に椅子が次のライド通知を受けること、想定外の状態遷移、長時間の配車待ちは失敗要因。

`task q`の同期済みDBには最新RUNが入っていなかったため、同じ読み取り専用DuckDBから
`read_text('runs/.../run.json')`、`read_text('runs/.../bench.log')`、`read_json('runs/.../alp.json')`を使用した。
分析DBの同期、成果物の再生成、計測基盤の変更はしていない。

| RUN | 実装 | 合否・スコア | 評価API 2xx | matching API件数／平均応答 |
| --- | --- | --- | ---: | --- |
| 20261011-051806 | オンメモリ化前、`tue 6357cdf` | PASS・45,032 | 678 | 77件／0.176秒 |
| 20261011-052401 | WALオンメモリ、`thu 3ab44a8` | PASS・185,859 | 2,736 | 105件／0.067秒 |

052401のGo実装は起点885e40eと同一（`git diff 3ab44a8..885e40e -- webapp/go`に差分なし）。
API集計はRUN全期間であり、評価API成功を公式採点区間の完了数と同一視しない。
051806の比較窓05:18:15〜05:19:15は完了51回、完了間隔平均1.16秒・p95 2.04秒という既存Evidence。
その値をオンメモリ化後の損失時間として使わない。

052401について保存app journalを同じ60秒幅の05:24:10〜05:25:10 JSTへ限定すると、
matching HTTP 204完了ログは62件、61区間の平均は0.888秒、p95は0.996秒、平均処理時間は0.0334秒だった。
観測された最初・最後の完了は05:24:10.253〜05:25:04.430。
これはjournalの記録時刻による呼出し完了間隔であり、ライドごとの配車待ちや採点60秒とは異なる。
外部HTTPS curlの完了後にsleepする構造は、処理のオンメモリ化後も残っていた。

052401の参加者INFOではマッチ不満が負荷序盤の0%から38.3%、52.3%、67.4%などへ増え、最終49.7%。
最終の迎車不満30.8%、移動不満59.4%も併せて見る。割合の母数と判定閾値は不明。
不満割合や配車呼出し間隔だけで資源の律速・改善効果量は断定しない。

両RUNの`run.json`はfinalized・`passed=true`、全artifact statusは`ok`、トップレベルの`missing`は未記録。
ただし両方ともローカルに`raw/access-isucon-1.log.zst`がない。
`task artifacts-run RUN=runs/20261011-052401`も必須圧縮accessログの欠損で失敗した。
保存済みのbench/app journal・alp・user-transitionsは存在する。欠損をアクセス0件として扱わない。

比較窓のクエリは次のとおり。

```shell
task q -- "WITH lines AS (
  SELECT unnest(string_split(content,chr(10))) AS line
  FROM read_text('runs/20261011-052401/isucon-1-app-journal.log')
), matches AS (
  SELECT try_cast(split_part(line,' ',1) AS TIMESTAMPTZ) AS reported_at,
    try_cast(regexp_extract(line,'in ([0-9.]+)(ms|µs|s)$',1) AS DOUBLE) *
      CASE regexp_extract(line,'in ([0-9.]+)(ms|µs|s)$',2)
        WHEN 's' THEN 1 WHEN 'ms' THEN 0.001 ELSE 0.000001 END AS response_s
  FROM lines WHERE contains(line,'GET http://isuride.xiv.isucon.net/api/internal/matching')
    AND contains(line,' - 204 ')
), windowed AS (
  SELECT *,epoch(reported_at-lag(reported_at) OVER (ORDER BY reported_at)) AS interval_s
  FROM matches WHERE reported_at >= TIMESTAMPTZ '2026-10-11 05:24:10+09'
    AND reported_at < TIMESTAMPTZ '2026-10-11 05:25:10+09'
)
SELECT count(*) AS completions,avg(response_s) AS avg_response_s,
  avg(interval_s) AS avg_completion_interval_s,
  quantile_cont(interval_s,0.95) AS p95_completion_interval_s
FROM windowed;"
```

## 実装と起動条件

専用goroutineと容量1の起動channelを追加した。最初のイベントから固定20ms待って状態を読み、
既存Hungarian配車を実行する。イベント追加で締切を延長しない。待機中はポーリングしない。
バッチ後も待機ライドと空き椅子が両方残れば、追加の20ms待ちを挟まず次の最大64件を実行する。
バッチ間でlifecycle lockを解放し、initializeが入れるようにする。

| 条件 | 起動の扱い |
| --- | --- |
| ライド作成 | SQL確定と作成WALの成功・状態公開後に起動 |
| 椅子登録 | 初期登録は必ずinactiveなので候補は増えない。続くactive化が起動点 |
| 椅子active化 | activity WAL成功後、既存の`available(..., true)`を満たせば起動 |
| 完了と通知消費 | 完了は未送信COMPLETEDを追加するため、完了だけでは再利用不可。chair ACKのWAL成功後、全通知が消費済みで再利用可能なら起動 |
| ACKの順序が入れ替わる場合 | 最後の未送信通知がCOMPLETED以外でも、全通知の消費が確定すれば起動 |
| 再起動 | SQLとWALを復元し、worker開始時に起動。待機ライドを新規イベントなしで処理 |
| initialize | 既存の`stateLifecycle`書込みlockでバッチを遮断。復元完了後に起動し、次のバッチは新世代の状態を取得 |
| 64件上限超過 | 両側に残りがあればworkerが連続実行。椅子不足なら次の再利用・active化・作成イベントを待つ |
| 座標更新 | 空き判定を変えない。位置不明でも既存のfallback候補として割当可能なので、追加の起動は不要 |
| DB投影backlog上限 | 配車WALを追加できなかった場合、既存投影workerが上限からbacklogを減らした確定後に起動 |

20msは最初の評価用の固定値であり、最適値を計測して決めたものではない。
即時起動より競合ライド・空き椅子の候補を集める一方、従来の500ms sleepより短い待ちにする。
混雑時のmutex待ち・Hungarian計算・WAL書込み時間は20msに含まれない。
小さいバッチが増えると全体での組合せ品質が落ちる可能性と、Hungarian呼出し・全ライド走査の回数増加を評価対象にする。
待機ライドが0件なら椅子候補の走査は省略する。

古いライド優先（created_at、id）と候補の安定順序を維持する。
状態mutexの下で候補取得・割当・WAL書込み・公開を直列化し、二重割当を防ぐ。
WAL追加に失敗した割当を状態やSSEへ公開しない。既存のWAL永続化方式は変更しない。
終了時は投影workerと同じ停止通知を配車workerにも送り、終了待ち後にWALを閉じる。

## デプロイ構成と影響

外部curlループのunit資材・setup取得・起動宣言・内部HTTP matching endpointを削除した。
`Taskfile.yml`の変更はcontest values内のサービス列とrole指定だけ。
競技用Taskfileと`tools/contest/deployments.yaml`へ`event-matching`の正規planを追加した。

将来の初回反映には`task deploy-event-matching`を使う。
全3ホストの既存matcherをdisable --nowしてMainPID=0を確認した後、isucon-1へGoアプリを配布・再起動する。
単独の`task deploy-app`はmatcher停止を含まないため、初回移行にはこのplanが必要。
通常のdeploy-all、apply-roles、db-recreateにもmatcher無効化を組み込み、再有効化・再起動宣言を削除した。
新しい互換フラグ、旧配車の併存、計測カウンターは追加していない。

今回実行したのは`task deploy-event-matching-dry`のみ。SSH・転送・サービス操作は実行していない。
対象はisucon-1のGoアプリと、isucon-1/2/3の外部matcher停止・無効化。payment mockの動作は維持する。

## ローカル検証

- `task test-app`：`go test -race ./...`成功。
  新しいworkerテストで130件の自動継続・FIFO・二重割当防止、作成後の起動、登録inactiveとactive化、
  完了後の全通知ACKによる再利用、集約されたHungarianの最適割当、initializeによる旧世代の遮断・終了を確認。
  既存のWAL失敗時の未公開、Hungarianと総当たりoracleの一致、通知・状態遷移・復元テストも通過。
  MySQL DSN未指定の統合テストはskip。実DBを伴うinitialize・投影の検証は今回行っていない。
- `task build TARGET_OS=linux TARGET_ARCH=amd64`成功。
  既存setupで確認済みのLinux x86_64に合わせ、出力は静的リンクELF x86-64と確認。
- `task deploy-event-matching-dry`成功。宣言を解釈し、アプリactivationの依存先がmatcher-disableであることと全対象ホストを確認。
- `git diff --check`成功。

## 次の単独RUNの評価条件

効果は未計測。売上集計など他の改善と混ぜず、最新のオンメモリ基準実装と比較する。
各RUNのartifact status・ローカルmissing・合否を先に確認する。
結果の得点、参加者INFOの進捗、API件数を同じRUNと定義で比較し、全期間集計と60秒窓を混ぜない。

| 判断 | 既存Evidenceと評価条件 |
| --- | --- |
| 配車待ち削減 | `bench_dissatisfaction` / `bench_progress`のマッチ不満の推移。負荷成長の進捗と同時刻で見る。読取りで得られるride_statusesのMATCHING→ENROUTE時間は通知・椅子ACKを含む代理指標として扱う |
| 移動・回転・得点寄与 | 評価API 2xx件数、座標API件数・時間、最終スコア。PASSを前提に、評価の増加と得点の増加が伴うか。評価API件数を採点完了数へ直接換算しない |
| バッチの質 | 迎車不満と迎車区間の時間、移動不満、評価・低評価離脱の推移を配車待ちと併せて比較。局所バッチのHungarian最適性は維持したが、20msで分割した実走行の候補数・総コストは既存保存RUNにないため未計測 |
| 資源と増える仕事 | 同じ時間窓のGo CPU秒・待ちと座標応答。呼出し頻度増加がHungarian・全ライド走査・状態mutex待ちを増やし、移動を遅らせていないか |
| 正当性 | 通知順序・完了前再割当・二重割当・配車タイムアウトなどのベンチエラーがないこと。通常再起動後の受理済みデータ取得、initialize後の世代更新も公式確認の対象 |

マッチ不満が改善しても迎車や移動・評価・スコアが悪化する場合、集約窓とバッチ分割の影響を再評価する。
不満の母数や候補数を推測で補完せず、間接指標の限界を保持する。
配車HTTPを廃止したため、matching API件数・curl完了間隔は変更後の比較指標に使わない。

## 054248評価後の限定修正：集約窓200ms

初回20ms版はmainの`fcf344d`を取り込み、`ddcdd4b`で本番へ反映済み。
ユーザー実行の`20261011-054248`はPASS・126,305点で、必須成果物の内容検査も成功した。
直前`20261011-053536`の225,300点から約44%低下した。
評価API成功は3,526→1,664件、最終マッチ不満は87.5→3.8%、迎車不満は15.2→57.3%、移動不満は38.1→66.0%。
API件数はRUN全期間、不満率は最終参加者INFOの値。053536はローカル圧縮accessログが欠損しているため保存集計を利用した。

保存済みDuckDBの`user_transition_edges`では、近隣椅子検索→ライド一覧が964→24,291件、
近隣椅子検索→料金見積りが4,314→2,013件だった。
両RUNのload window開始後5〜65秒の同じ60秒窓では、2コア容量120 CPU秒に対して
ホスト稼働97.13→109.33 CPU秒、nginx 33.28→49.92 CPU秒、Go 28.88→34.93 CPU秒。
CPU値は既存`metrics`のサンプル間隔を窓境界で切り、割合と実時間を積分した。
座標APIのRUN全期間平均応答は26→51msに悪化した。

空き椅子をすぐ割り当てることで、待ちの一部がライド作成前へ移り、検索の繰返しを増やした可能性がある。
小さいバッチによる割当品質低下も候補であり、原因は未確定。
ユーザー承認の限定修正として、`matchingCoalesceWindow`だけを20msから200msへ変更した。
固定締切・容量1の起動集約・残りバッチの即時継続・Hungarian・FIFO・通知ACKによる再利用条件・WAL公開順序は維持する。

候補を集める時間を増やして迎車と検索の繰返しを改善し、従来の500ms sleepより短い待ちを狙う。
追加の最大180msの集約待ちとのトレードオフがある。200ms版の性能・スコアへの効果は未計測。
次の単独RUNで、マッチ不満・迎車/移動不満の推移、検索の繰返し、座標応答、評価API成功件数、
スコアを同じ定義で比較する。採用判断はマッチ不満だけでなく、225,300点を超えるかと正当性を併せて行う。

今回の200ms変更はローカルのみ。ユーザーの指示によりデプロイを待つ。
`task test-app`（race付き）と`task build TARGET_OS=linux TARGET_ARCH=amd64`、`git diff --check`は成功。
MySQL DSN未指定の統合テストはskip。新しいテスト・計測・設定フラグは追加していない。


## 055518評価後：変更前の配車起動へ戻す

ユーザー実行の`20261011-055518`はPASS・175,935点。全artifact statusはokで、
`task artifacts-run`の必須成果物の内容検査も成功した。
同じ売上差分集計を含む変更前`20261011-054939`（PASS・217,486点）と比較すると、
スコアは19.1%低下、評価API成功は3,364→2,414件（28.2%低下）だった。
両実装の`owner_handlers.go`・`runtime_sales.go`・SQLスキーマに差分はない。
054939のローカル圧縮accessログは欠損しているため、保存済み集計を利用した。

最終参加者INFOのマッチ不満は86.4→46.2%、迎車不満は16.8→50.2%、移動不満は36.3→58.9%。
近隣椅子検索→ライド一覧の遷移は1,125→7,099件に増えた。
API・遷移件数はRUN全期間の集計であり、評価API成功件数を採点完了数へ直接換算しない。
各実装1RUNの比較で原因は未確定だが、配車待ちの改善に評価・スコアの増加が伴わず、採用を見送る。

ユーザーの依頼により、`0764217`と`b5fd227`の実装変更を戻した。
専用worker・起動channel・起動通知・即時の残りバッチ処理を撤去し、内部matching APIと
外部matcherのcurl→sleep起動、unit資材・Taskfile・サービス役割・既存deployment planを復元した。
Hungarian配車、古いライド優先、完了通知消費後の再利用条件、mutexによる二重割当防止、
WAL確定後の公開、およびmainから取り込んだ売上差分集計は維持した。
実験記録とRUN成果物は残す。

今回はmainへのマージ準備のローカル変更。デプロイ・サービス操作・ベンチ・mainへのマージは行っていない。
本番は200ms版のままであり、次に復元版を反映するときはアプリ配布に加えて既存の
dependencies/roles構成で外部matcherを再起動・有効化する必要がある。
復元版のスコアは未計測。


復元後のローカル検証：

- `task test-app`（`go test -race ./...`）成功。配車の順序・排他・通知条件と売上集計の既存テストを含む。
  MySQL DSN未指定の統合テストはskip。
- `task build TARGET_OS=linux TARGET_ARCH=amd64`成功。静的リンクELF x86-64を確認。
- `task --list`でTaskfileを正常に解釈。
- 現在の`main 8d7ea22`と比較し、`webapp/go`・`Taskfile.contest.yml`・
  `tools/contest/deployments.yaml`・matcher unitは差分なし。
  `Taskfile.yml`の今回の差分はcontest values内のmatcherサービス宣言の復元のみ。
- `git diff --check`、`git diff --cached --check`成功。
