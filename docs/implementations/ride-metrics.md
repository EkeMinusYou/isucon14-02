# ライド・座標・配車の計測契約

Goアプリの既存`/debug/sql-pools`から固定名の集計値を公開し、既存sql-pool collector、TSV importer、`tools/contest/analysis-schema/ride-metrics.sql`で取得・分析する。IDラベル、リクエスト単位ログ、観測値のWAL保存は追加しない。割当方式、費用式、呼出頻度、WAL形式・確定条件、通知順序、ACK・再利用条件を維持する。

## epoch・snapshot・欠損

現行は`ride_metrics_version=3`、正常なsnapshotは固定452項目、各名前32文字以内。v1の`pickup_b9_64`は9〜64件、v2/v3の`pickup_b9_plus`は9件以上。旧RUNはそのversionの語彙だけで読む。共通collectorはアプリ固有名を持たず、数値項目の独立した`-max-metrics`上限（既定1024）を使用する。pool上限128、750msのHTTP timeout、応答1MiB、TSV出力32MiBの既存上限は維持する。collector binaryの更新も必要。

`ride_metrics_epoch`はプロセス起動・restore/initialize成功時のUnixマイクロ秒由来tokenで、同一プロセス内では必ず前回より大きい。WALのgenerationとは別物。通常再起動でも履歴を観測counterへ再加算せず、新しいepochの0から始める。復元時のゲージは現在状態から一度だけ構築する。

snapshotはlifecycle→共通状態→既存評価集計mutexの順序で取得する。状態ロックはTryRLockを500µs間隔で試し、開始から20msを期限にする（OS schedulingによる超過は取得時間へ記録）。取得後は固定サイズのhistogram/counter・占有集計だけをコピーし、ロックを解放してからscalar mapを展開する。過去履歴・椅子全件の毎秒走査や、バックグラウンドのsnapshot再生成は行わない。

| snapshot項目 | 契約 |
| --- | --- |
| `snapshot_status` | 0=整合した取得、1=lifecycle混雑、2=restore未完了/metricsなし、3=状態ロック取得期限、4=既存評価集計mutex混雑 |
| `ride_metrics_ready` | 1=このepochの整合した集計値を今回取得。0の場合、epoch・業務counter/gaugeを出さない |
| `ride_state_available` | metricsを取得できた場合のWAL利用可否。WAL失敗後もready=1で失敗counterを観測でき、available=0となる |
| `snapshot_observed_us` | 成功時は固定集計をコピーする直前のUnix µs。失敗時は取得断念時刻。キャッシュ値を新しい時刻で再公開しない |
| `snapshot_acquire_ms` | endpointのsnapshot関数入口→固定集計のコピー・ロック解放までの経過ms。取得待ちを含む |
| `snapshot_export_ms` | 同入口→scalar map展開完了までの経過ms。JSON送信・ネットワーク時間は含まない |

collectorの`captured_unix_ms`はSQL pool取得後の別の時刻。SQLの`ride_metric_samples.ts`はこの収集時刻、`observed_at`はアプリ集計の観測時刻、`capture_age_ms`は両者の差。ミリ秒丸めによりageが最大約1ms負になる場合がある。v3の`ride_metric_intervals.ts`・ゲージ・histogramのtsはobserved_atで、別列`captured_at`に収集時刻を残す。Unix µsは`make_timestamptz`で変換し、JSTのセッションでも同じ瞬間を扱う。

busy/unavailableは取得失敗の新しい境界であり、0件の観測ではない。HTTP timeoutも従来どおりcollectorのstderr/失敗statusに残る。snapshot busyのHTTP応答が成功しても、業務計測のcoverageが100%とはみなさず、readyサンプル数・status・有効差分秒数を確認する。基準074654はsql-poolが2 timeout、coverage97.1%、failed/invalid。原因を共通ロックと断定しない。

初回、epoch/version変更、ready=0、欠落field、counter減少、観測時刻の非増加をまたぐ差分はNULL。SQLはversion別の固定項目gridを使い、series全体の欠落もNULLとして残す。丸ごと収集されなかった時間を補間せず、同epochの前後が有効なら実際の長いintervalの差分として扱う。狭い窓へ按分しない。0は取得できた0件、NULL/行なしは算出不能・欠損。

## ① 座標更新とWAL

時間は全て経過msであり、CPU時間ではない。`coordinate()`呼出順の1,65,129,…を測る固定1/64サンプリング。順序counterだけはatomicで、initializeをまたいでもプロセス内のcadenceを維持する。対象件数・標本counterとhistogramはepochごとにresetする。固定間引きの標本には偏り得るため、sum×64を実測CPU秒や厳密な全呼出し時間と呼ばない。

| 項目・series | 境界・母数 |
| --- | --- |
| `coord_calls_ok/fail` | coordinate関数に到達し、成功/エラーでreturnした全呼出し数。認証・JSON検証等で関数に到達しないHTTP失敗は含まない |
| `coord_wal_ok/fail` | 座標レコードの完全なWAL書込み成功/書込み・生成エラー数。WAL利用不能の事前拒否、存在しない椅子はWAL試行に含めない |
| `coord_apply_ok` | 書込み成功後の状態適用回数 |
| `coord_samples_ok/fail` | sampled呼出しの関数結果別件数。`coord_sample_every=64`も公開 |
| `coord_wait_ok/fail` | Lock直前→取得直後。分類はcoordinate関数全体の結果 |
| `coord_hold_ok/fail` | 取得直後→return直前。検証、レコード生成、WAL、適用、通知publishを含む。観測集計自体とUnlockを除く |
| `coord_json_ok/fail` | JSON marshalとレコードサイズ検査。分類はこの段階の結果 |
| `coord_frame_ok/fail` | frame確保、長さ・CRC・本文の構築 |
| `coord_write_ok/fail` | File.Writeの呼出しとshort write検査。background fsyncや応答送信は含めない |
| `coord_apply_ok/fail` histogram | 完全書込み後のapply呼出し。現在applyはerrorを返さないためfailは0。事前拒否・書込み失敗で未到達なら標本を追加しない |
| `coord_sample_bytes` | sampled File.Writeが実際に返した書込みbyte数の合計。失敗時の部分書込みを含む |

時間seriesは`_n/_sum/_b0/_b1/_b2/_b3/_inf`、境界0.1/1/10/100ms/+∞。stage成功の後に別stageが失敗しても先行stageのokを取り消さない。各stageの母数が異なり、JSON以降の時間はhold内に重複する。平均値同士を加算して全HTTP時間としない。

状態所有の固定histogram/counterを既に保持中の共通状態ロック内で更新する。座標hot pathに新しい共通集計mutexを加えない。WALの内容・順序は変えず、完全なFile.Writeの後だけ距離・状態を適用する。background fsyncの周期と通常再起動時の復元処理も維持する。

## ② 実測段階・占有・受理済み距離

状態は椅子単位で保持し、公開ラベルは固定stageだけ。割当のWAL書込み成功後、状態適用中に開始する。

| stage | 起点→終点 | 主に見分ける時間 |
| --- | --- | --- |
| `assigned` | 割当済みMATCHING→ENROUTE受理 | 割当通知・椅子の開始待ち |
| `enroute` | ENROUTE→pickup座標受理によるPICKUP | 迎車の実測移動区間 |
| `pickup` | PICKUP→CARRYING受理 | 乗車・通知待ち |
| `carrying` | CARRYING→目的地座標受理によるARRIVED | 乗車の実測移動区間 |
| `arrived` | ARRIVED→COMPLETED確定 | 決済・評価・完了待ち |
| `reuse` | COMPLETED→既存matching可能条件成立 | 全椅子宛ACK完了とactive待ち |
| `unknown` | 割当済みだが既知statusのない状態→既知状態 | 状態境界が不明な占有 |

移動区間はサーバー受理時刻間の経過時間で、移動中の座標HTTP応答待ちやクライアントの次動作までの待ちを含む。物理的に走行した時間だけを復元するものではない。`flow_<stage>` histogramは、このepochで入口から出口まで観測したstageだけを集計する。全サイクル`flow_cycle`は割当→再配車可能を全て観測した椅子だけ。境界は既存時間histogramと同じ10/100/1000/3000ms/+∞。

`flow_<stage>_chairs`は現在その段階にいる椅子数、`_age_ms`は現在の椅子の段階滞在時間合計（chair-ms）、`_total_ms`はepoch開始以降の完了・進行中を合わせた占有積分（chair-msの累積counter）。差分total/1000が窓内のchair秒、total差分/interval_msが平均占有椅子数。完了ライドだけの偏りを避け、窓末の進行中も含める。`flow_ongoing_chairs/age_ms`は割当→再利用の進行中件数・経過合計、`flow_ongoing_known/restored`で起点既知と復元分を分ける。snapshotで椅子・ライド履歴は走査せず、遷移時の件数・epoch相対入口時刻合計から算出する。

復元された未完了、完了後のACK待ち、最後のrideが完了済みでinactiveの椅子はepoch開始からの部分観測とする。元の入口時刻を推定しない。`flow_restored_chairs`、`flow_<stage>_partial`、`flow_cycle_partial`で部分観測を数え、部分stage/cycleをfull histogramへ混ぜない。復元後に入口と出口を観測した次stageはfullになる。過去のidle完了履歴を毎秒走査したり、新規完了cycleとして加算したりしない。

`flow_dist_enroute/carrying`は、成功した座標レコード適用時に、直前の永続状態とのManhattan距離差分を、その適用前のstatusに加算する。pickup/到着点への最後の差分も含む。単位は座標距離単位、histogram境界0/1/10/100/+∞、nはbaseline既知の当該段階の受理済み座標数。初回座標は距離不明として`flow_coord_first`へ、rideなしは`flow_coord_no_ride`、他段階は`flow_coord_other`へ分類する。距離0の再送・同位置更新も受理数に含め、`flow_coord_zero`で別途数える（このcounterは他区分と重複）。失敗更新は距離を増やさない。未知baselineを0距離サンプルへ置き換えない。

距離は改善比較用の観測値であり、公式採点の再現値ではない。採点窓、進行中ライドの採点、端数処理、座標再送と採点の対応は未確定。推定迎車`pickup_b1/b2_8/b9_plus`（割当時の位置と速度の推定ms）と実測`flow_enroute`は別指標。

再利用はactive・未完了rideなし・全椅子宛通知ACK済みという既存matching条件。app ACK、評価HTTP応答完了、nearbyの100ms猶予とは別。再送・重複ACK・不正status・失敗WALでstageをもう一度終了しない。既存`assign_flush_chair/app`は割当後MATCHINGの最初のサーバーFlush成功だけを記録し、受信確認やACK成功とは区別する。失敗ACK後の再送でも初回flushを重複計上しない。

## ③ ready機会・計画単位

readyは同じ公開状態の`waitingRides>0 && matchingChairs>0 && WAL利用可 && ready`。成立時刻と中断を状態変更のcommit後にO(1)で更新する。generation変更は新しい機会にし、古い時刻を持ち越さない。

| 項目 | 母数・境界 |
| --- | --- |
| `match_ready/ready_age_ms/ready_total_ms` | 連続した需要・供給の共存状態、現区間の経過ms、共存時間の累積ms |
| `match_pending/pending_age_ms` | まだ計画開始へ渡されていない共存機会と、その待ち。solver終了後も需要・供給が残れば新しいpending区間を開く |
| `match_ready_version` | ready成立/終了とgeneration切替で増えるepoch内の観測世代 |
| `match_ready_open/interrupted/generation` | 共存成立、未処理のまま中断、generation切替の件数 |
| `match_ready_wait` | 当該pending成立→計画試行開始（観測用Lock直前）。非空計画だけが母数。始点より後にready再成立した場合は0msで`match_ready_after_start`を数える |
| `match_calls` / `match_mu` | context事前拒否を通過してmatchへ入りmatchingMuを取得した呼出し数 / matchingMuのLock直前→取得直後。待機中の未取得呼出しはまだ計上しない |
| `match_attempts` | 各snapshot試行数。HTTP呼出しの追加バッチ、最大3回の再計画、空・下限未満・利用不能のsnapshotを含む |
| `match_rides/candidates` | 非空snapshotの計画ride数・コピー候補椅子数。retryした計画も含み、割当成功件数とは別。境界1/8/64/256/+∞ |
| `match_start_lock` | pendingをclaimする短い観測用状態Lockの取得待ち |
| `match_snapshot_lock/snapshot` | 元のsnapshot用RLockの待ち / 取得後の検証・値コピー。空snapshotも含む |
| `match_solver` | snapshot後の候補ソートとsolver。非空計画が母数 |
| `match_commit_lock/commit` | commitMatchingの共通状態Lock待ち / 取得後の検証・WAL・適用・既存計測・通知。commitへ到達した計画が母数 |
| `match_total_ok/retry/empty/fail` | 試行開始→結果決定。成功、同generation競合、空/下限未満/コピー前generation不一致、利用不能/WAL失敗/非空計画のgeneration断念を分ける |
| `match_batches/assigned` | commit成功した非空バッチ数 / 実割当ride数。既存dispatch_batch/dispatch_waitとも照合できる |
| `match_conflict_rides/chairs` | FIFO ride集合変更・別割当済み / 椅子inactive・未完了ride・ACK競合の再計画件数 |
| `match_generation/unavailable/wal_fail` | generation変更による断念 / 状態利用不能 / 確定WAL失敗の件数 |
| `match_empty_demand/empty_supply/below_min` | 空snapshot理由の件数 |
| `match_abandon_limit` | 1回のmatchBatchで3回とも競合し、次の外部呼出しへ延期した件数 |
| `match_planning` | snapshot・solver・commit・空試行の結果集計までにある進行中試行数 |

空/下限未満のsnapshotでready_waitを増やさず、同じready世代のpending始点を戻す。中断・再成立があればその新しい始点を使う。短い観測用の状態LockをHTTP呼出し/試行開始/空試行の終了に追加するが、scheduler、追加バッチの下限、solver、最大retry数は変更しない。stageの経過時間は重複するためCPU秒へ換算・加算しない。

## 既存評価指標

`dispatch_wait`は保存済みCreatedAt→割当commit成功直後のms、`dispatch_batch`は成功バッチのride件数。`dispatch_waiting/oldest_ms/available/notify_blocked`は現在の未割当、最古待ち、配車可能、完了済みだが未ACK通知で除外されたactive椅子数。現在indexと同じ条件で差分更新する。

`eval_total_ok/fail`は認証後handler入口→終了、okはCOMPLETED確定成功。`eval_lock_ok`はuser mutex待ち、`eval_sql_ok/fail`は同期SQL呼出し合計、`eval_payment_ok/fail`は決済関数（現行POST再試行と待機を含む）、`eval_complete_ok/fail`はcomplete呼出しの経過ms。到達したstageだけを、そのstageの結果で数える。`eval_payment_ok/fail_retries`は追加POST試行数。平均の母数は同じstage・結果のnで、各stageの平均時間を足してHTTP時間としない。

## 負担・確認・次のユーザーRUN

座標ごとに追加するのはatomic順序counter、固定counter、受理距離のO(1)集計。追加の時計は時間サンプル1/64の呼出しと状態境界で使う。状態遷移は椅子1台分の占有と固定histogramを更新する。RAMは既存椅子index内の時刻/フラグ、固定histogram/counterに増える。WAL byte数は変わらない。snapshotは1秒ごとに452scalarを展開し、collectorはその分TSV行数が増える。CPU・応答・coverage・得点への影響量は未測定。最初のユーザーRUNで負担と正当性を評価する。

`task test-ride-metrics`でsampling/成功失敗/WAL未適用、段階と距離・初回座標、ACK/activity再利用、reset/部分観測、占有積分、ready中断/再成立/generationと計画理由、snapshot欠損/concurrent race、実HTTP→collector→TSV→DuckDB、v1/v2/v3項目grid・全series欠損・JST観測時刻を確認する。`task test-app`は既存WAL・通知・状態テストをrace付きで実行する。DSN未指定のMySQL統合テストは既存どおりskip。Linux/amd64は`task build-ride-metrics TARGET_OS=linux TARGET_ARCH=amd64`を使用する（既存buildと内部build-sql-pool-metricsタスクを呼ぶ）。

想定反映先はisucon-1のGoアプリとsql-pool collector。分析schema/queryはローカル。nginx2/3、MySQL3、DB配置・schema・初期化資材を変更しない。本実装作業はローカルだけで、デプロイ・service操作・RUN/分析DB同期を行わない。

後日のRUNをユーザーが実行・finalizeした後、通常の同期で新schemaを反映し、同一RUN・窓を指定する。以下のqueryはartifact status/coverage、snapshot品質、各指標の有効秒数・NULL区間、占有chair秒、CPU秒と観測/窓capacity、両入口の座標応答と配車/評価HTTP、招待・離脱の原文、得点/PASSを返す。

```sh
task artifacts-run RUN=runs/RUN_ID
task q -- "SET VARIABLE ride_run='RUN_ID'; $(cat tools/contest/queries/ride-metrics.sql)"
```

INFO起点+30〜60秒で比較する場合は、同queryの前に`ride_window_start/end`を設定する（SQLは同じセッションで実行する）。

```sql
SET VARIABLE ride_window_start=(SELECT min(reported_at)+INTERVAL '30 seconds'
 FROM bench_log_lines WHERE run_id=getvariable('ride_run') AND contains(line,'負荷走行を開始'));
SET VARIABLE ride_window_end=(SELECT min(reported_at)+INTERVAL '60 seconds'
 FROM bench_log_lines WHERE run_id=getvariable('ride_run') AND contains(line,'負荷走行を開始'));
```

histogram/counterは両端が窓内の有効区間だけを集計し、それぞれの有効観測秒数を分母にする。stage別の秒数を横断して足さない。CPUは既存resource_demandと同じ1サンプル=1秒の積分で、観測capacityと指定窓capacityを併記する。hostとserviceのCPU秒は重複するので加算しない。HTTPはバケット開始で窓へ選択し、入口ごとの件数を足しても距離・採点ライド数には置き換えない。ゲージは採取点の値であり、その単純平均を連続時間の平均占有と呼ばない。
