(ns Thorin.bittorrent-sybil
  (:require
   [clojure.core.async :as a :refer [chan go go-loop <! >!  take! put! offer! poll! alt! alts! close! onto-chan!
                                     pub sub unsub mult tap untap mix admix unmix pipe
                                     timeout to-chan  sliding-buffer dropping-buffer
                                     pipeline pipeline-async]]
   [clojure.core.async.impl.protocols :refer [closed?]]
   [cljs.core.async.interop :refer-macros [<p!]]
   [clojure.pprint :refer [pprint]]
   [clojure.string]

   [Thorin.bytes]
   [Thorin.codec]
   [Thorin.datagram-socket]
   [Thorin.protocols]
   [Thorin.bencode]
   [Thorin.seed]))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(declare process-socket)

(defn process
  [{:as opts
    :keys [stateA
           nodes-bootstrap
           stop|
           sybils|
           infohash|
           count-messages-sybilA]}]
  (let [already-sybiledA (atom {})
        self-idBA (Thorin.bytes/random-bytes 20)
        self-id (Thorin.codec/hex-to-string self-idBA)

        port 6882
        host "0.0.0.0"
        
        nodes| (chan (sliding-buffer 100000)
                     (comp
                      (filter (fn [node]
                                (and (not= (:host node) host)
                                     (not= (:id node) self-id)
                                     #_(not= 0 (js/Buffer.compare (:id node) self-id))
                                     (< 0 (:port node) 65536))))
                      (filter (fn [node] (not (get @already-sybiledA (:id node)))))
                      (map (fn [node] [(:id node) node]))))

        msg| (chan (sliding-buffer 1024)
                   (keep (fn [{:keys [msgBA host port]}]
                           (try
                             {:msg (Thorin.bencode/decode msgBA)
                              :host host
                              :port port}
                             (catch Exception ex nil)))))
        msg|mult (mult msg|)

        send| (chan 100)

        send-krpc-request (Thorin.seed/send-krpc-request-fn {:msg|mult msg|mult})

        routing-tableA (atom {})]

    (process-socket
     {:msg| msg|
      :send| send|
      :host host
      :port port})
    
    (go
      (<! (onto-chan! sybils| (map (fn [i]
                                     (Thorin.bytes/random-bytes 20))
                                   (range 0 (Thorin.seed/fixed-buf-size sybils|))) true))
      (doseq [node nodes-bootstrap]
        (take!
         (send-krpc-request
          {:t (Thorin.bytes/random-bytes 4)
           :y "q"
           :q "find_node"
           :a {:id self-idBA
               :target (Thorin.seed/gen-neighbor-id self-idBA (Thorin.bytes/random-bytes 20))}}
          node
          (timeout 2000))
         (fn [{:keys [msg] :as value}]
           (when value
             (when-let [nodesBA (get-in msg [:r :nodes])]
               (let [nodes (Thorin.seed/decode-nodes nodesBA)]
                 (swap! routing-tableA merge (into {} (map (fn [node] [(:id node) node]) nodes)))
                 (onto-chan! nodes| nodes false)))))))

      (loop [n 16
             i n]
        (let [timeout| (when (= i 0)
                         (timeout 500))
              [value port] (alts!
                            (concat
                             [stop|]
                             (if timeout|
                               [timeout|]
                               [sybils|]))
                            :priority true)]
          (condp = port

            timeout|
            (recur n n)

            sybils|
            (when-let [sybil-idBA value]
              (let [state @stateA
                    [id node] (<! nodes|)]
                (swap! already-sybiledA assoc id true)
                (take!
                 (send-krpc-request
                  {:t (Thorin.bytes/random-bytes 4)
                   :y "q"
                   :q "find_node"
                   :a {:id sybil-idBA
                       :target (Thorin.seed/gen-neighbor-id (:idBA node) self-idBA)}}
                  node
                  (timeout 2000))
                 (fn [{:keys [msg] :as value}]
                   (when value
                     (when-let [nodesBA (get-in msg [:r :nodes])]
                       (let [nodes (Thorin.seed/decode-nodes nodesBA)]
                         (onto-chan! nodes| nodes false)))))))
              (recur n (mod (inc i) n)))

            stop|
            (do :stop)))))


    (let [msg|tap (tap msg|mult (chan (sliding-buffer 512)))]
      (go
        (loop []
          (when-let [{:keys [msg host port] :as value} (<! msg|tap)]
            (let [msg-y (some-> (:y msg) (Thorin.bytes/to-string))
                  msg-q (some-> (:q msg) (Thorin.bytes/to-string))]
              (cond

                (and (= msg-y "q")  (= msg-q "ping"))
                (let [txn-idBA  (:t msg)
                      node-idBA (get-in msg [:a :id])]
                  (if (or (not txn-idBA) (not= (Thorin.bytes/alength node-idBA) 20))
                    (do nil :invalid-data)
                    (put! send| {:msg {:t txn-idBA
                                       :y "r"
                                       :r {:id (Thorin.seed/gen-neighbor-id node-idBA self-idBA)}}
                                 :host host

                                 :port port})))

                (and (= msg-y "q")  (= msg-q "find_node"))
                (let [txn-idBA  (:t msg)
                      node-idBA (get-in msg [:a :id])
                      target-idBA (get-in msg [:a :target])]
                  (if (or (not txn-idBA) (not= (Thorin.bytes/alength node-idBA) 20))
                    (println "invalid query args: find_node")
                    (put! send| {:msg {:id (Thorin.seed/gen-neighbor-id node-idBA self-idBA)
                                       :nodes (Thorin.seed/encode-nodes (take 8 @routing-tableA))}
                                 :host host

                                 :port port})))

                (and (= msg-y "q")  (= msg-q "get_peers"))
                (let [infohashBA (get-in msg [:a :info_hash])
                      txn-idBA (:t msg)
                      node-idBA (get-in msg [:a :id])
                      tokenBA (-> (Thorin.bytes/buffer-wrap infohashBA 0 4) (Thorin.bytes/to-byte-array))]
                  (if (or (not txn-idBA) (not= (Thorin.bytes/alength node-idBA) 20) (not= (Thorin.bytes/alength infohashBA) 20))
                    (println "invalid query args: get_peers")
                    (do
                      (put! infohash| {:infohashBA infohashBA})
                      #_(send-krpc
                         socket
                         (clj->js
                          {:t txn-idB
                           :y "r"
                           :r {:id (Thorin.seed/gen-neighbor-id infohashB self-idB)
                               :nodes (Thorin.seed/encode-nodes (take 8 @routing-tableA))
                               :token tokenB}})
                         rinfo))))

                (and (= msg-y "q")  (= msg-q "announce_peer"))
                (let [infohashBA   (get-in msg [:a :info_hash])
                      txn-idBA (:t msg)
                      node-idBA (get-in msg [:a :id])
                      tokenBA (-> (Thorin.bytes/buffer-wrap infohashBA 0 4) (Thorin.bytes/to-byte-array))]
                  (cond
                    (not txn-idBA)
                    (println "invalid query args: announce_peer")

                    #_(not= (-> infohashB (.slice 0 4) (.toString "hex")) (.toString tokenB "hex"))
                    #_(println "announce_peer: token and info_hash don't match")

                    :else
                    (do
                      #_(send-krpc
                         socket
                         (clj->js
                          {:t txn-idB
                           :y "r"
                           :r {:id (Thorin.seed/gen-neighbor-id infohashB self-idB)}})
                         rinfo)
                      (put! infohash| {:infohashBA infohashBA}))))

                :else
                (do nil)))


            (recur)))))))

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

          evt|
          ([{:keys [op] :as value}]
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