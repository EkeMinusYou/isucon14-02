# 飽和時のみ64件配車を最大2バッチ処理する

対象は会話 `docs/conversation/20261011-052356_01a12779-8958-7a10-8679-90adf6eec46f.md`
の2026-10-11 06:26:32の案①。起点はmain `f55908f`。

## 実装

- 1バッチ目は従来どおりFIFO先頭最大64件を既存Hungarianで割り当てる。
- 成功後、待機ライドと現在の配車可能椅子が各64件以上残る場合だけ追加バッチへ進む。
- バッチ間で共通状態ロックを解放する。取り直したロック内で初期化世代と両集合の件数を再確認し、そのままsolver・WAL・通知まで実行する。
- 最大2バッチで終了する。他のmatching呼出しが間に入ることは許容し、各バッチはその時点のFIFO先頭を使う。
- ACK待ち・未完了・非稼働の除外、モデル速度と迎車／乗車時間の評価式、WAL成功後の公開・通知を維持する。
- WALと既存メトリクスはバッチ単位。`dispatch_batch_n`はHTTP呼出し数ではなく実際のバッチ数、`dispatch_batch_sum`は割当件数を表す。新しいcounter／logは追加しない。
- 外部matcherの500ms待機、solverのロック範囲、バッチサイズは維持する。

## 基準と成果物の状態

基準RUN `20261011-061703` はfinalized、279,537点、PASS。測定commitは`115fd36`。
DuckDBの`load_windows`はtraffic-threshold起点21:17:10Z、64秒。
この起点の30〜60秒で、既存`ride_metric_gauges`の30サンプル全てが
waiting >= 64かつavailable >= 64。平均waiting 2019.3、available 343.93を`task q`で再確認した。

`run.json`のINFO負荷開始窓は21:17:07.304Zから69.502秒で、上記の窓とは異なる。
CPUの基準最大値62.8%／76.1%／38.9%はrun.json窓の値であり、DuckDB窓の値と混ぜない。
平均だけからCPU余力や改善幅を断定しない。

sql-pool成果物は実在するが、2回timeoutによりstatus failed、quality invalid、coverage 97.1%。
取得済みの有効な値だけを使い、欠損を0として扱わない。
このワークツリーの`task artifacts-run RUN=runs/20261011-061703`は
`raw/access-*.log.zst`／`raw/access-isucon-2.log.zst`の実体欠損で失敗する。
run.jsonにはaccess成果物がokとして記録されているが、ローカル実在確認とは区別する。成果物は再生成しない。

## 検証と次回RUNの判定

`task test-matching`でrace検出付きの閾値・FIFO・同時呼出し・ACK条件・
追加バッチ前の空き不足／待機消費／世代変更／利用不可状態／WAL失敗／座標更新・2バッチのWAL復元を確認する。
`task build TARGET_OS=linux TARGET_ARCH=amd64`でGoアプリをクロスコンパイルする。

配布対象はGoアプリのhost1。1呼出し内の処理とWAL書込みが飽和時に最大2回になるため、
座標更新の待ちとCPUピークへの影響は実測が必要。host2のmatcher設定やhost3のDB設定は変更しない。

次回はこの案だけでPASS・score、後半のwaiting／oldest・割当件数/秒・バッチサイズ、
pickup推定時間／不満・移動／完了・椅子成長を比較する。
CPU最大／p95／pressureとcoordinate API p99も、基準と同じ窓を明示して比較する。
HTTP呼出し数と64件バッチ回数は分け、counter差分の区間端と欠損を確認する。
デプロイ・サーバー状態変更・ベンチ実行は今回の作業に含めない。

## main取り込みと配布

ローカル実装後に、ユーザーからmain取り込みとデプロイの指示を受けた。
main `0f0c618`をmonへマージ（`c937948`）し、採用済みのWAL投影削減を取り込んだ。
削除された投影待ち配列に代わり、バッチごとのWAL記録をディスクから読む検証へ変更した。
2バッチ化の実装と既存メトリクスは維持する。

取り込み後の`task test-matching`、`task test-app`はrace付きで成功。
`task build TARGET_OS=linux TARGET_ARCH=amd64`も成功した。
`task deploy-app-dry`でisucon-1への配布・既存WALを使ったGoサービス再起動を確認し、
`task check-roles`で3台のサービス配置を確認した。

配布前のdeployctl verifyは、isucon-1のGoとisucon-2のnginxに別ワークツリー由来の差分を報告した。
このワークツリーとの一致を得るため、共通操作ロック内で`task deploy-app`と`task deploy-nginx`を実行し、成功した。
nginxの設定・静的資材はこのワークツリーのmain相当へそろえ、isucon-2のnginxをreloadする。
DB再作成、MySQL再起動、matcher変更、ベンチ実行は行わない。

配布commitは`c19d029`。配布後のdeployctl verifyは3台全て一致、
`task check-roles`と`task check-network`も成功した。
