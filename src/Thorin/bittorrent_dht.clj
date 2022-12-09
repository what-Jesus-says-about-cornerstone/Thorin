(ns Thorin.bittorrent-dht
  (:require
   [clojure.core.async :as a :refer [chan go go-loop <! >!  take! put! offer! poll! alt! alts! close!
                                     pub sub unsub mult tap untap mix admix unmix pipe
                                     timeout to-chan  sliding-buffer dropping-buffer
                                     pipeline pipeline-async]]
   [clojure.core.async.impl.protocols :refer [closed?]]
   [clojure.string]
   [Thorin.bytes]
   [Thorin.codec]
   [Thorin.bencode]

   [Thorin.datagram-socket]
   [Thorin.protocols]

   [Thorin.seed]))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defn process-socket
  [{:as opts
    :keys [msg|
           send|
           host
           port]}]
  (let [ex| (chan 1)
        evt| (chan (sliding-buffer 10))
        socket (Thorin.datagram-socket/create
                {:host host
                 :port port
                 :evt| evt|
                 :msg| msg|
                 :ex| ex|})
        release (fn []
                  (Thorin.protocols/close* socket))]
    (go
      (Thorin.protocols/listen* socket)
      (<! evt|)
      (println (format "listening on %s:%s" host port))
      (loop []
        (alt!
          send|
          ([{:keys [msg host port] :as value}]
           (when value
             (Thorin.protocols/send*
              socket
              (Thorin.bencode/encode msg)
              {:host host
               :port port})
             (recur)))

          #_evt|
          #_([{:keys [op] :as value}]
             (when value
               (cond
                 (= op :listening)
                 (println (format "listening on %s:%s" host port)))
               (recur)))

          ex|
          ([ex]
           (when ex
             (release)
             (println :ex ex)
             (println :exiting)))))
      (release))))

(defn process-messages
  [{:as opts
    :keys [stateA
           msg|mult
           send|
           self-idBA
           infohashes-from-listening|]}]
  (let [msg|tap (tap msg|mult (chan (sliding-buffer 512)))]
    (go
      (loop []
        (when-let [{:keys [msg host port] :as value} (<! msg|tap)]
          (let [msg-y (some-> (:y msg) (Thorin.bytes/to-string))
                msg-q (some-> (:q msg) (Thorin.bytes/to-string))]
            (cond

              #_(and (= msg-y "r") (goog.object/getValueByKeys msg "r" "samples"))
              #_(let [{:keys [id interval nodes num samples]} (:r (js->clj msg :keywordize-keys true))]
                  (doseq [infohashBA (->>
                                      (js/Array.from  samples)
                                      (partition 20)
                                      (map #(js/Buffer.from (into-array %))))]
                    #_(println :info_hash (.toString infohashBA "hex"))
                    (put! infohash| {:infohashBA infohashBA
                                     :rinfo rinfo}))

                  (when nodes
                    (put! nodesBA| nodes)))


              #_(and (= msg-y "r") (goog.object/getValueByKeys msg "r" "nodes"))
              #_(put! nodesBA| (.. msg -r -nodes))

              (and (= msg-y "q")  (= msg-q "ping"))
              (let [txn-idBA  (:t msg)
                    node-idBA (get-in msg [:a :id])]
                (if (or (not txn-idBA) (not= (Thorin.bytes/alength node-idBA) 20))
                  (do nil :invalid-data)
                  (put! send|
                        {:msg  {:t txn-idBA
                                :y "r"
                                :r {:id self-idBA #_(Thorin.seed/gen-neighbor-id node-idB (:self-idBA @stateA))}}
                         :host host
                         :port port})))

              (and (= msg-y "q")  (= msg-q "find_node"))
              (let [txn-idBA  (:t msg)
                    node-idBA (get-in msg [:a :id])]
                (if (or (not txn-idBA) (not= (Thorin.bytes/alength node-idBA) 20))
                  (println "invalid query args: find_node")
                  (put! send|
                        {:msg {:t txn-idBA
                               :y "r"
                               :r {:id self-idBA #_(Thorin.seed/gen-neighbor-id node-idB (:self-idBA @stateA))
                                   :nodes (Thorin.seed/encode-nodes (take 8 (:routing-table @stateA)))}}
                         :host host
                         :port port})))

              (and (= msg-y "q")  (= msg-q "get_peers"))
              (let [infohashBA (get-in msg [:a :info_hash])
                    txn-idBA (:t msg)
                    node-idBA (get-in msg [:a :id])
                    tokenBA (Thorin.bytes/copy-byte-array infohashBA 0 4)]
                (if (or (not txn-idBA) (not= (Thorin.bytes/alength node-idBA) 20) (not= (Thorin.bytes/alength infohashBA) 20))
                  (println "invalid query args: get_peers")
                  (do
                    (put! infohashes-from-listening| {:infohashBA infohashBA})
                    (put! send|
                          {:msg {:t txn-idBA
                                 :y "r"
                                 :r {:id self-idBA #_(Thorin.seed/gen-neighbor-id infohashBA (:self-idBA @stateA))
                                     :nodes (Thorin.seed/encode-nodes (take 8 (:routing-table @stateA)))
                                     :token tokenBA}}
                           :host host
                           :port port}))))

              (and (= msg-y "q")  (= msg-q "announce_peer"))
              (let [infohashBA  (get-in msg [:a :info_hash])
                    txn-idBA (:t msg)
                    node-idBA (get-in msg [:a :id])
                    tokenBA (Thorin.bytes/copy-byte-array infohashBA 0 4)]

                (cond
                  (not txn-idBA)
                  (println "invalid query args: announce_peer")

                  #_(not= (-> infohashBA (.slice 0 4) (.toString "hex")) (.toString tokenB "hex"))
                  #_(println "announce_peer: token and info_hash don't match")

                  :else
                  (do
                    (put! send|
                          {:msg {:t tokenBA
                                 :y "r"
                                 :r {:id self-idBA}}
                           :host host
                           :port port})
                    #_(println :info_hash (.toString infohashBA "hex"))
                    (put! infohashes-from-listening| {:infohashBA infohashBA}))))

              :else
              (do nil)))

          (recur))))))

(defn valid-for-ping?
  [[id node]]
  (or
   (not (:pinged-at node))
   (> (- (Thorin.seed/now) (:pinged-at node)) (* 2 60 1000))))

(defn process-routing-table
  [{:as opts
    :keys [stateA
           self-idBA
           routing-table-nodes|
           send-krpc-request
           routing-table-max-size]}]
  (let [routing-table-comparator (Thorin.seed/hash-key-distance-comparator-fn self-idBA)
        stop| (chan 1)]

    (swap! stateA update :routing-table (partial into (sorted-map-by routing-table-comparator)))

    ; add nodes to routing table
    (go
      (loop [n 4
             i 0
             ts (Thorin.seed/now)
             time-total 0]
        (when-let [nodes (<! routing-table-nodes|)]
          (let [routing-table (:routing-table @stateA)]
            (->>
             nodes
             (transduce
              (comp
               (filter (fn [node]
                         (not (get routing-table (:id node))))))
              (completing
               (fn [result node]
                 (assoc! result (:id node) node)))
              (transient {}))
             (persistent!)
             (swap! stateA update :routing-table merge))

            ; trim routing table periodically
            (if (and (>= i 4) (> time-total 4000))
              (let [nodes (:routing-table @stateA)
                    nodes-near (take (* 0.9 routing-table-max-size) nodes)
                    nodes-far (take-last
                               (- (min (count nodes) routing-table-max-size) (count nodes-near))
                               nodes)]
                (->>
                 (concat nodes-near nodes-far)
                 (into (sorted-map-by routing-table-comparator))
                 (swap! stateA assoc :routing-table))
                (recur n 0 (Thorin.seed/now) 0))
              (recur n (inc i) (Thorin.seed/now) (+ time-total (- (Thorin.seed/now) ts)))))))
      (close! stop|))

    ; ping nodes and remove unresponding
    (go
      (loop []
        (alt!
          (timeout (* 15 1000))
          ([_]
           (let [state @stateA]
             (doseq [[id node] (->>
                                (:routing-table state)
                                (sequence
                                 (comp
                                  (filter valid-for-ping?)
                                  (take 8))))]
               (take! (send-krpc-request
                       {:t (Thorin.bytes/random-bytes 4)
                        :y "q"
                        :q "ping"
                        :a {:id self-idBA}}
                       node
                       (timeout 2000))
                      (fn [value]
                        (if value
                          (swap! stateA update-in [:routing-table id] assoc :pinged-at (Thorin.seed/now))
                          (swap! stateA update-in [:routing-table] dissoc id))))))
           (recur))

          stop|
          (do :stop))))

    ; peridiacally remove some nodes randomly 
    #_(let [stop| (chan 1)]
        (swap! procsA conj stop|)
        (go
          (loop []
            (alt!
              (timeout (* 30 1000))
              ([_]
               (->> (:routing-table @stateA)
                    (keys)
                    (shuffle)
                    (take (* 0.1 (count (:routing-table @stateA))))
                    (apply swap! stateA update-in [:routing-table] dissoc))
               (recur))

              stop|
              (do :stop)))))))


(defn process-dht-keyspace
  [{:as opts
    :keys [stateA
           self-idBA
           dht-keyspace-nodes|
           send-krpc-request
           routing-table-max-size]}]
  (let [routing-table-comparator (Thorin.seed/hash-key-distance-comparator-fn self-idBA)
        stop| (chan 1)]
    (swap! stateA merge {:dht-keyspace (into {}
                                             (comp
                                              (map
                                               (fn [char-str]
                                                 (->>
                                                  (repeatedly (constantly char-str))
                                                  (take 40)
                                                  (clojure.string/join ""))))
                                              (map (fn [k] [k {}])))
                                             ["0"  "2"  "4"  "6"  "8"  "a"  "c"  "e"]
                                             #_["0" "1" "2" "3" "4" "5" "6" "7" "8" "9" "a" "b" "c" "d" "e" "f"])})
    (doseq [[id routing-table] (:dht-keyspace @stateA)]
      (swap! stateA update-in [:dht-keyspace id] (partial into (sorted-map-by (Thorin.seed/hash-key-distance-comparator-fn (Thorin.codec/hex-to-bytes id))))))
    (swap! stateA update :dht-keyspace (partial into (sorted-map)))

    ; add nodes to routing table
    (go
      (loop [n 4
             i 0
             ts (Thorin.seed/now)
             time-total 0]
        (when-let [nodes (<! dht-keyspace-nodes|)]
          (let [dht-keyspace-keys (keys (:dht-keyspace @stateA))]
            (doseq [node nodes]
              (let [closest-key (->>
                                 dht-keyspace-keys
                                 (sort-by identity (Thorin.seed/hash-key-distance-comparator-fn (:idBA node)))
                                 first)]
                (swap! stateA update-in [:dht-keyspace closest-key] assoc (:id node) node)))

            ; trim routing tables periodically
            (if (and (>= i 4) (> time-total 4000))
              (do
                (swap! stateA update :dht-keyspace
                       (fn [dht-keyspace]
                         (->>
                          dht-keyspace
                          (map (fn [[id routing-table]]
                                 [id (->> routing-table
                                          (take routing-table-max-size)
                                          (into (sorted-map-by (Thorin.seed/hash-key-distance-comparator-fn (Thorin.codec/hex-to-bytes id)))))]))
                          (into (sorted-map)))))
                (recur n 0 (Thorin.seed/now) 0))
              (recur n (inc i) (Thorin.seed/now) (+ time-total (- (Thorin.seed/now) ts)))))))
      (close! stop|))

    ; ping nodes and remove unresponding
    (go
      (loop []
        (alt!
          (timeout (* 15 1000))
          ([_]
           (let [state @stateA]
             (doseq [[k routing-table] (:dht-keyspace state)
                     [id node] (->>
                                routing-table
                                (sequence
                                 (comp
                                  (filter valid-for-ping?)
                                  (take 8))))]
               (take! (send-krpc-request
                       {:t (Thorin.bytes/random-bytes 4)
                        :y "q"
                        :q "ping"
                        :a {:id self-idBA}}
                       node
                       (timeout 2000))
                      (fn [value]
                        (if value
                          (swap! stateA update-in [:dht-keyspace k id] assoc :pinged-at (Thorin.seed/now))
                          (swap! stateA update-in [:dht-keyspace k] dissoc id))))))
           (recur))

          stop|
          (do :stop))))))