# 配車・再利用待ちと評価・決済の計測

isucon-friでmain `535fdf8`を起点に、合意済みの候補1・2をローカル実装。対象は現在の`APP_HOSTS=isucon-1`のGoプロセス。配車起動、一括割当の選択、通知順序・再送・ACK・再利用条件、決済のリトライ、SQL/WAL永続化は変更しない。デプロイ・サービス操作・DB再作成・ベンチ・mainへのマージは行わない。

## 出力と世代

既存`measurement.Options.Snapshot`→`/debug/sql-pools`→SQL pool collector（既定1秒）→`*-sql-pool-metrics.tsv`→既存`metrics`テーブルを使う。独自メトリクスのentityは`local:metrics:snapshot:0`。新collector、IDラベル、リクエスト単位ログは追加しない。固定名124個、各32文字以内で既存collectorの128個制限内。

`ride_metrics_version=1`、`ride_metrics_epoch`（起動または正常なrestore完了時のUnixマイクロ秒由来のtoken、プロセス内では前回より必ず大きい）、`ride_metrics_ready`を出す。世代はWAL世代とは別で、同じDB/WALからの通常再起動でもカウンターを0から始める。初期化のrestore時もカウンター・配信計測をリセットする。復元済みのライドや通知を新しい割当・初回flushとして計上しない。ゲージだけは復元状態から一度構築する。利用不能時はready=0のみを出し、他の値を0で埋めない。

競技ビューは`tools/contest/analysis-schema/ride-metrics.sql`。`ride_metric_samples`（採取境界）、`ride_metric_definitions`（単位・固定区分・bucket）、`ride_metric_intervals`（差分）、`ride_metric_gauges`、`ride_metric_histograms`を提供する。既存sourceの意味・単位は変更しない。

初回、epoch変更、ready/version不一致、カウンター減少、直前サンプルにそのメトリクスがない場合の差分はNULL。収集失敗で行がない時間を補間しない。復帰後に同じepochの値が連続している場合、差分は実際の長い区間に対する量となり、`interval_ms`も実際の採取時間差を示す。0は観測された0件、NULL/行なしは算出不能・未収集。負荷窓に入る最初のサンプルには直前値がない場合があり、その区間を推定しない。

## 固定区分、起終点、分母

各histogramの接尾辞は`_n`（観測件数）、`_sum`（観測値の合計）、`_b0`〜`_b3`・`_inf`（上限以下の累積件数）。平均は同じ区間・epoch・区分のsum/n。時間histogramの単位はms、境界は10 / 100 / 1000 / 3000 / +∞。正確なp99を復元するものではなく、その値が属するbucketを判定する。`dispatch_batch`だけは値・sumがライド件数、境界は1 / 8 / 32 / 64 / +∞、nの分母は成功した非空の割当バッチ数。

| 指標・series | 定義と分母 |
| --- | --- |
| `dispatch_waiting` | 椅子ID未割当の現在ライド数 |
| `dispatch_oldest_ms` | 未割当ライドの最古の保存済みCreatedAtからSnapshot時点まで。待ち0件なら0ms |
| `dispatch_available` | activeかつ全ライドCOMPLETEDかつ全椅子宛通知ACK済みの椅子数。近隣一覧の評価応答・100ms猶予とは別の既存配車条件 |
| `dispatch_notify_blocked` | activeかつ全ライドCOMPLETEDだが過去を含む椅子宛通知に未送信記録が残る椅子数。未完了ライドのある椅子・inactive椅子は含めない |
| `dispatch_wait` | 保存済みRide.CreatedAt→割当WAL commit成功直後の時刻。SQLでの作成・公開前の時間も含む。成功した新規割当1件が分母 |
| `dispatch_batch` | commit成功バッチの実割当ライド件数。候補数・未割当全件数ではない |
| `pickup_b1` / `pickup_b2_8` / `pickup_b9_64` | 同一割当バッチの実件数1 / 2〜8 / 9〜64ごとの迎車距離÷モデル速度×1000ms。椅子の割当前位置からpickupまでのManhattan距離。既知の位置・正の速度を持つ割当が分母。実際の到着時間ではない |
| 同上の`_unknown` | 位置・速度不明の割当件数。推定値の0として扱わない。既知histogramのnと合わせると区分の全割当件数 |
| `assign_flush_chair` / `assign_flush_app` | 割当commit成功直後→割当MATCHINGイベントのSSE Flush成功直後。椅子/appを固定区分にする。割当をこのepochで観測し、その宛先へのflush成功を初めて記録したライド数が分母 |
| `eval_total_ok/fail` | 認証後の評価handler入口→handler終了のdefer。JSON検証・応答flushと利用者mutex解放も含む。COMPLETED確定成功をokとし、応答flush失敗によってfailへ変えない。handlerへ到達した評価が分母 |
| `eval_lock_ok` | userMutexを得た後のLock直前→Lock取得直後。取得完了した評価が分母。Lock自体は失敗を返さないためfail区分なし |
| `eval_sql_ok/fail` | payment_tokensとsettingsの同期GetContext各呼出し時間の合計。プール待ちを含む。両方成功でok、1本目/2本目のエラーでfail。1回以上呼んだ評価が分母、SQL文数ではない |
| `eval_payment_ok/fail` | 決済関数呼出し直前→return。POST、曖昧な結果のGET照合・ライド取得、全100ms再試行待ちを含む。関数のerror有無で判定。決済を呼んだ評価が分母 |
| `eval_payment_ok_retries` / `eval_payment_fail_retries` | 上記と同じ区分の再試行回数合計。初回POSTは含めず、追加試行0〜5回を加算。平均再試行数の分母は同じ区間・区分のeval_payment n |
| `eval_complete_ok/fail` | complete呼出し直前→return。状態mutex待ち、検証、COMPLETED/統計のWAL確定・通知publishを含む。error有無で判定。completeを呼んだ評価が分母。okのnが完了確定件数 |

各stageはそのstage自身の成功・失敗で分類し、到達しなかったstageにはサンプルを追加しない。SQL成功後の決済失敗はSQL ok・payment fail・total failとなる。stageの分母は異なるため平均時間同士を足してAPI平均とみなさない。時計は経過時間に単調時計を使うが、保存済みCreatedAtからの待ちと復元ゲージは壁時計。負の差は0に制限する。

SSEのflush成功はサーバーでのflush成功であり、クライアント受信確認ではない。ACKの成功とは独立してRAM上の初回記録を保持し、ACK失敗後の再送・同時接続・Last-Event-ID再送を別の初回として計上しない。flush失敗は件数を増やさない。複数接続で成功した場合、状態ロック内で最初に記録した成功を採用する。ACK失敗後でもflush成功までの時間は記録し、再利用ゲージは既存ACK条件どおり解除しない。appのMATCHINGは割当前に送信済みの場合があり、その送信は計上しない。割当後に同イベントが実際にflushされたときだけapp区分に入り、椅子区分と同じ分母とは限らない。未flushの割当はflush分布の分母にまだ入らないため、割当件数との差は時間窓末尾の未配信も含み、失敗件数とは断定できない。

## 追加コストと確認

未割当ライドだけのheapで作成・割当時O(log W)、最古ゲージはO(1)。椅子のactive/未完了ライド数/未送信状態数を状態変化時に差分更新し、座標のみの更新には計測処理を追加しない。再起動・初期化時だけ現在状態と通知履歴を走査し、インデックスを構築する。割当時はバッチと割当ライド数に比例した集計、初回flushは時計1回・短い状態ロックと集計ロック、評価成功経路は追加時計取得12回・評価終了時の集計ロック1回。既存業務処理内の時刻取得は別。Snapshotは1秒ごとに124個のscalarを出力するための短いstate読取りロックと集計ロック・map構築を行う。RAMは待ちライド・椅子のインデックス、各ライドの初回flush状態、固定histogramに増える。負荷時のCPU・得点への影響量は未計測で、次のユーザーRUNで確認する。

`task test-ride-metrics`はWAL成功/失敗、待ち/椅子ゲージ遷移、重複flushとACK、世代リセット、未知の迎車推定、評価SQL失敗とmutex待ち、既存決済の成功・曖昧成功・2回再試行・5回失敗を確認する。RAM fixtureの実Snapshot→実HTTP exporter→変更していない既存collector→実TSV importer→競技DuckDBビューも通し、124個の制限・欠損値・世代変更のNULL差分を確認する。fixtureは一時ディレクトリで消え、RUN成果物を生成しない。`task test-app`でrace付きGoテスト、`task build TARGET_OS=linux TARGET_ARCH=amd64`で既存setupで確認済みの対象へクロスビルドする。MySQL統合テストはDSN指定なしでは既存どおりskipし、今回は同期SQL経路をローカルdriver fixtureで確認する。

## 次のユーザーRUN

反映する際の想定対象はisucon-1のGoアプリと、ローカル分析schema参照。Go反映には正規のdeploy-appによるrestartが伴う。配車や決済の設定・DBスキーマ変更はない。本依頼では反映しない。

ユーザーが次RUNを実行・finalizeした後、通常の`task q-sync`で既存TSVと競技ビューを同期して以下を読む（ここでは同期も行わない）。RUN_IDは実測したRUNに置き換える。

```sh
task artifacts-run RUN=runs/RUN_ID
task q -- "SET VARIABLE ride_run='RUN_ID'; $(cat tools/contest/queries/ride-metrics.sql)"
```

SQLはartifact品質・負荷窓とreadyサンプル数、ゲージ、同一epoch内の完備したhistogram区間の件数/合計/平均/bucket、未知推定・retry件数、割当/完了の時系列を返す。行なし・NULLを0件に補わず、missing_intervalsと採取範囲も読む。長いgapを含む区間を狭い時間窓へ分配しない。ゲージの平均・最大は採取点での値であり、全時刻の平均・最大ではない。CPU秒・実行可能数・ベンチ進捗と比較する場合も同じRUN/ホスト/負荷窓で合わせる。30〜60秒などに絞る場合は同じload_windowsをその窓へ限定し、両端が窓内のintervalだけを比較する。

配車イベント起動の評価では、作成→割当と割当→flush、空き椅子/通知待ち、バッチサイズ分布、バッチ3区分の迎車推定平均と未知率を併せて比較する。評価の判断には各stageの件数・合計ms・bucketと決済のretry回数、完了成功件数を使う。秒境界で終了したstageを計上するため、進行中の評価の時間はまだ含まれない。CPU高負荷やAPI平均だけで原因を断定しない。

基準RUN `20261011-053536`のEvidenceは依頼に記載済みの225300点/PASS。このworktreeの分析DBにはそのRUNのmetricsが未同期（0行）だったため、成果物の再生成・同期による再集計は行っていない。run.jsonでartifactがokでも、ローカルraw/access-isucon-1.log.zstは欠損している。未収集・欠損は0として扱わず、manifestとローカルの存在をそれぞれ確認する。

## main統合とデプロイ（2026-10-11）

ユーザーの「mainを取りこんでデプロイして」の依頼を受け、main `741e4b5`を`81dcf48`で統合。その作業中に追加されたmain `0057de7`（現在状態index・通知queue）も`4375d00`で取り込んだ。売上累積集計・現在状態index・通知queueと計測更新を両方維持して競合を解消した。配車はmainの外部一括配車を維持し、通知の初回flush判定をqueueのイベント生成へ組み込んだ。追加テストで未割当数・配車可能数とmainのindexの一致、未ACK通知の再送を初回として重複集計しないことを確認した。

`task test-app`（race付き、collector→DuckDB fixtureを含む）、`task build TARGET_OS=linux TARGET_ARCH=amd64`がPASS。既存の未コミットworktree hook設定は一時保全して復元し、mainに同じ変更が入っていたため最終的にcleanとなった。

全worktree共通の操作ロック内で、進行中RUNマーカーなし、全ホストのrole一致、collector停止を確認して`task deploy-app`を実行。最終配布コードは`4375d00`、対象はisucon-1のGoアプリとisuride-goのunit、サービスを正常再起動した。最初の配布では直後の疎通確認が復元中で失敗したが、約2秒後に待受開始を確認。最新mainの再配布では起動後の内部疎通を待ってから`task setup-postflight`を実行し、全ホストのrole、nginx→app・app→MySQL、配布内容のworktree一致、collector停止、CPU/fgprof停止を確認した。実ホストの`/debug/sql-pools`で固定124個・ready=1・version=1を確認した。

DB再作成・SQL資材の変更・ベンチ・分析RUN成果物の再生成は行っていない。mainブランチ自体は変更していない。得点・負荷時の影響はユーザーの次RUNで確認する。
