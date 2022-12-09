(ns Thorin.bittorrent-sample-infohashes
  (:require
   [clojure.core.async :as a :refer [chan go go-loop <! >!  take! put! offer! poll! alt! alts! close! onto-chan!
                                     pub sub unsub mult tap untap mix admix unmix pipe
                                     timeout to-chan  sliding-buffer dropping-buffer
                                     pipeline pipeline-async]]
   [clojure.core.async.impl.protocols :refer [closed?]]
   [Thorin.bytes]
   [Thorin.codec]
   [Thorin.seed]))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defn process-sampling
  [{:as opts
    :keys [stateA
           self-idBA
           nodes-to-sample|
           nodes-from-sampling|
           infohashes-from-sampling|
           send-krpc-request]}]
  (let [stop| (chan 1)

        node-to-sample| (chan 32)
        node-from-sampling| (chan 32)]

    (pipe nodes-to-sample| node-to-sample| true)
    (pipe nodes-from-sampling| node-from-sampling| true)

    (go
      (loop [n 4
             i n
             ts (Thorin.seed/now)
             time-total 0]
        (let [timeout| (when (and (== i 0) (< time-total 1000))
                         (timeout (+ time-total (- 1000 time-total))))
              [value port] (alts! (if timeout|
                                    [timeout|]
                                    [node-from-sampling| node-to-sample|])
                                  :priority true)]
          (when (or value (= port timeout|))
            (cond

              (= port timeout|)
              (do nil
                  (recur n n (Thorin.seed/now) 0))

              (or (= port node-from-sampling|) (= port node-to-sample|))
              (let [[id node] value]
                (swap! stateA update-in [:routing-table-sampled] assoc id (merge node
                                                                                 {:timestamp (Thorin.seed/now)}))
                (take! (send-krpc-request
                        {:t (Thorin.bytes/random-bytes 4)
                         :y "q"
                         :q "sample_infohashes"
                         :a {:id self-idBA
                             :target (Thorin.bytes/random-bytes 20)}}
                        node
                        (timeout 2000))
                       (fn [value]
                         (when value
                           (let [{:keys [msg host port]} value
                                 {:keys [interval nodes num samples]} (:r msg)]
                             (when samples
                               (doseq [infohashBA (Thorin.seed/decode-samples samples)]
                                 #_(println :info_hash (.toString infohashB "hex"))
                                 (put! infohashes-from-sampling| {:infohashBA infohashBA})))
                             (when interval
                               (swap! stateA update-in [:routing-table-sampled id] merge {:interval interval}))
                             (when nodes
                               (onto-chan! nodes-from-sampling| (Thorin.seed/decode-nodes nodes) false))))))
                (recur n (mod (inc i) n) (Thorin.seed/now) (+ time-total (- ts (Thorin.seed/now))))))))))))



; ask for infohashes, then for metadata using one tcp connection
#_(let [stop| (chan 1)
        nodes| (chan 1
                     (comp
                      (filter (fn [node]
                                (not (get (:routing-table-sampled @stateA) (:id node)))))))
        nodes|mix (mix nodes|)
        cancel-channelsA (atom [])
        release (fn []
                  (doseq [cancel| @cancel-channelsA]
                    (close! cancel|)))]
    (swap! procsA conj stop|)
    (admix nodes|mix nodes-to-sample|)
    (go
      (loop [n 8
             i n
             ts (Thorin.seed/now)
             time-total 0]
        (when (and (= i 0) (< time-total 2000))
          (a/toggle nodes|mix {nodes-to-sample| {:pause true}})
          (<! (timeout (+ time-total (- 2000 time-total))))
          (a/toggle nodes|mix {nodes-to-sample| {:pause false}})
          (recur n n (Thorin.seed/now) 0))
        (alt!
          nodes|
          ([node]
           (let []
             (swap! stateA update-in [:routing-table-sampled] assoc (:id node) (merge node
                                                                                      {:timestamp (Thorin.seed/now)}))
             (let [alternative-infohash-targetB (.randomBytes crypto 20)
                   txn-idB (.randomBytes crypto 4)]
               #_(println :sampling-a-node)
               (when-let [value (<! (send-krpc-request
                                     socket
                                     (clj->js
                                      {:t txn-idB
                                       :y "q"
                                       :q "sample_infohashes"
                                       :a {:id self-idB
                                           :target alternative-infohash-targetB}})
                                     (clj->js node)
                                     (timeout 2000)))]
                 (let [{:keys [msg rinfo]} value
                       {:keys [interval nodes num samples]} (:r (js->clj msg :keywordize-keys true))]
                   (when samples
                     (let [cancel| (chan 1)
                           _ (swap! cancel-channelsA conj cancel|)
                           infohashes (Thorin.seed/decode-samples samples)
                           _ (doseq [infohashB infohashes]
                               (put! infohash| {:infohashB infohashB
                                                :rinfo rinfo}))]
                       (doseq [infohashB infohashes]
                         (<! (timeout 500))
                         (take! (request-metadata node self-idB infohashB cancel|)
                                (fn [value]
                                  (println :result value))))

                       #_(println :torrents)
                       #_(pprint (<! (request-metadata-multiple node self-idB infohashes cancel|)))))
                   (when interval
                     (println (:id node) interval)
                     (swap! stateA update-in [:routing-table-sampled (:id node)] merge {:interval interval}))
                   #_(when nodes
                       (put! nodes-to-sample| nodes))))))

           (recur n (mod (inc i) n) (Thorin.seed/now) (+ time-total (- ts (Thorin.seed/now)))))

          stop|
          (do :stop)))
      (release)))