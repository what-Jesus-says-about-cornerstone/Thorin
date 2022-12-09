(ns Thorin.ut-metadata
  (:require
   [clojure.core.async :as a :refer [chan go go-loop <! >!  take! put! offer! poll! alt! alts! close! onto-chan!
                                     pub sub unsub mult tap untap mix admix unmix pipe
                                     timeout to-chan  sliding-buffer dropping-buffer
                                     pipeline pipeline-async]]
   [clojure.string]
   [Thorin.bytes]
   [Thorin.codec]
   [Thorin.bencode]
   [clojure.walk :refer [keywordize-keys]]))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defprotocol WireProtocol)

#_(defprotocol BufferCut
  (cut* [_ recv| expected-size]))

#_(defn buffer-cut
    []
    (let [buffersV (volatile! (transient []))
          total-sizeV (volatile! 0)]
      (reify
        BufferCut
        (cut*
          [_ recv| expected-size]
          (go
            (loop []
              (let [total-size @total-sizeV]
                (cond

                  (== total-size expected-size)
                  (let [resultB (if (== 1 (count @buffersV))
                                  (nth @buffersV 0)
                                  (->
                                   @buffersV
                                   (persistent!)
                                   (Thorin.bytes/concat)))]
                    (vreset! buffersV (transient []))
                    (vreset! total-sizeV 0)
                    resultB)

                  (> total-size expected-size)
                  (let [overB (Thorin.bytes/concat (persistent! @buffersV))
                        resultB (Thorin.bytes/buffer-wrap overB 0 expected-size)
                        leftoverB (Thorin.bytes/buffer-wrap overB expected-size (- total-size expected-size))]
                    (vreset! buffersV (transient [leftoverB]))
                    (vreset! total-sizeV (Thorin.bytes/capacity leftoverB))
                    resultB)

                  :else
                  (when-let [recvB (<! recv|)]
                    (vswap! buffersV conj! recvB)
                    (vreset! total-sizeV (+ total-size (Thorin.bytes/capacity recvB)))
                    (recur))))))))))

(defn buffer-cut
  [{:as opts
    :keys [:from|
           :expected-size|
           :to|
           :metadata|
           :close?]}]
  (go
    (loop [buffersT (transient [])
           total-size 0
           expected-size (<! expected-size|)]
      (when expected-size
        (cond
          (== total-size expected-size)
          (let [resultBB (if (== 1 (count buffersT))
                          (nth buffersT 0)
                          (->
                           buffersT
                           (persistent!)
                           (Thorin.bytes/concat)))]
            (>! to| resultBB)
            (recur (transient []) 0 (<! expected-size|)))

          (> total-size expected-size)
          (let [overBB (Thorin.bytes/concat (persistent! buffersT))
                resultBB (Thorin.bytes/buffer-slice overBB 0 expected-size)
                leftoverBB (Thorin.bytes/buffer-slice overBB expected-size (- total-size expected-size))]
            (>! to| resultBB)
            (recur (transient [leftoverBB]) (Thorin.bytes/capacity leftoverBB) (<! expected-size|)))

          :else
          (when-let [recvBB (<! from|)]
            (recur (conj! buffersT recvBB) (+ total-size (Thorin.bytes/capacity recvBB)) expected-size)))))
    (close! to|)))

(def pstrlenBA (Thorin.bytes/byte-array [19]))
(def pstrBA (Thorin.bytes/to-byte-array "BitTorrent protocol" #_"\u0013BitTorrent protocol"))
(def reservedBA (Thorin.bytes/byte-array [0 0 0 0 0 2r00010000 0 2r00000001]))
(def keep-aliveBA (Thorin.bytes/byte-array [0 0 0 0]))
(def chokeBA (Thorin.bytes/byte-array [0 0 0 1 0]))
(def unchokeBA (Thorin.bytes/byte-array [0 0 0 1 1]))
(def interestedBA (Thorin.bytes/byte-array [0 0 0 1 2]))
(def not-interestedBA (Thorin.bytes/byte-array [0 0 0 1 3]))
(def haveBA (Thorin.bytes/byte-array [0 0 0 5 4]))
(def portBA (Thorin.bytes/byte-array [0 0 0 3 9 0 0]))

(def ^:const ut-metadata-block-size 16384)
(def ^:const ut-metadata-max-size 1000000)

(defn extended-msg
  [ext-msg-id data]
  (let [payloadBA (->
                   data
                   (Thorin.bencode/encode))
        msg-lengthBB (Thorin.bytes/buffer-allocate 4)
        msg-length (+ 2 (Thorin.bytes/alength payloadBA))]
    (Thorin.bytes/put-uint32 msg-lengthBB 0 msg-length)
    (->
     (Thorin.bytes/concat
      [(Thorin.bytes/to-byte-array msg-lengthBB)
       (Thorin.bytes/byte-array [20 ext-msg-id])
       payloadBA]))))

(defn handshake-msg
  [infohashBA peer-idBA]
  (Thorin.bytes/concat [pstrlenBA pstrBA reservedBA infohashBA peer-idBA]))

(defn create
  [{:as opts
    :keys [:send|
           :recv|
           :metadata|
           :infohashBA
           :peer-idBA]}]
  (let [stateV (volatile!
                {})

        ex| (chan 1)

        expected-size| (chan 1)
        cut| (chan 1)

        wire-protocol
        ^{:type :wire-protocol}
        (reify
          WireProtocol
          clojure.lang.IDeref
          (deref [_] @stateV))

        release (fn []
                  (close! expected-size|))]

    (buffer-cut {:from| recv|
                 :expected-size| expected-size|
                 :to| cut|
                 :close? true})

    (take! ex|
           (fn [ex]
             #_(println :ex (ex-message ex))
             (release)
             (when (:ex| opts)
               (put! (:ex| opts) ex))
             #_(when-let [ex| (:ex| opts)]
                 (put! ex| ex))))

    (go
      (try
        (>! send| (handshake-msg infohashBA peer-idBA))

        (loop [stateT (transient
                       {:expected-size 1
                        :op :pstrlen
                        :pstrlen nil
                        :msg-length nil
                        :am-choking? true
                        :am-interested? false
                        :peer-choking? true
                        :peer-interested? false
                        :peer-extended? false
                        :peer-infohashBA nil
                        :peer-dht? false
                        :extensions {"ut_metadata" 3}
                        :peer-extended-data {}
                        :ut-metadata-downloaded 0
                        :ut-metadata-max-rejects 0
                        :ut-metadata-pieces (transient [])})]
          (>! expected-size| (:expected-size stateT))
          (when-let [msgBB (<! cut|)]

            (condp = (:op stateT)

              :pstrlen
              (let [pstrlen (Thorin.bytes/get-uint8 msgBB 0)]
                (recur (-> stateT
                           (assoc! :op :handshake)
                           (assoc! :pstrlen pstrlen)
                           (assoc! :expected-size (+ 48 pstrlen)))))

              :handshake
              (let [{:keys [pstrlen]} stateT
                    pstr (-> (Thorin.bytes/buffer-slice msgBB 0 pstrlen) (Thorin.bytes/to-string))]
                (if-not (= pstr "BitTorrent protocol")
                  (throw (ex-info "Peer's protocol is not 'BitTorrent protocol'"  {:pstr pstr} nil))
                  (let [reservedBB (Thorin.bytes/buffer-slice msgBB pstrlen 8)
                        infohashBB (Thorin.bytes/buffer-slice msgBB (+ pstrlen 8) 20)
                        peer-idBB (Thorin.bytes/buffer-slice msgBB (+ pstrlen 28) 20)]
                    #_(println :received-handshake)
                    (>! send| (extended-msg 0 {:m (:extensions stateT)
                                               #_:metadata_size #_1000}))
                    (recur (-> stateT
                               (assoc! :op :msg-length)
                               (assoc! :expected-size 4)
                               (assoc! :peer-infohashBA (Thorin.bytes/to-byte-array infohashBB))
                               (assoc! :peer-extended? (not (== 0 (bit-and (Thorin.bytes/get-uint8 reservedBB 5) 2r00010000))) )
                               (assoc! :peer-dht? (not (== 0 (bit-and (Thorin.bytes/get-uint8 reservedBB 7) 2r00000001)))))))))

              :msg-length
              (let [msg-length (Thorin.bytes/get-uint32 msgBB 0)]
                (if (== 0 msg-length) #_:keep-alive
                    (do
                      (recur stateT))
                    (recur (-> stateT
                               (assoc! :op :msg)
                               (assoc! :msg-length msg-length)
                               (assoc! :expected-size msg-length)))))

              :msg
              (let [stateT (-> stateT
                               (assoc! :op :msg-length)
                               (assoc! :expected-size 4))
                    {:keys [msg-length]} stateT
                    msg-id (Thorin.bytes/get-uint8 msgBB 0)]

                (cond

                  #_:choke
                  (and (== msg-id 0) (== msg-length 1))
                  (recur (-> stateT
                             (assoc! :peer-choking? true)))

                  #_:unchoke
                  (and (== msg-id 1) (== msg-length 1))
                  (recur (-> stateT
                             (assoc! :peer-choking? false)))

                  #_:interested
                  (and (== msg-id 2) (== msg-length 1))
                  (recur (-> stateT
                             (assoc! :peer-interested? true)))

                  #_:not-interested
                  (and (== msg-id 3) (== msg-length 1))
                  (recur (-> stateT
                             (assoc! :peer-interested? false)))

                  #_:have
                  (and (== msg-id 4) (== msg-length 5))
                  (let [piece-index (Thorin.bytes/get-uint32 msgBB 1)]
                    (recur stateT))

                  #_:bitfield
                  (== msg-id 5)
                  (recur stateT)

                  #_:request
                  (and (== msg-id 6) (== msg-length 13))
                  (let [index (Thorin.bytes/get-uint32 msgBB 1)
                        begin (Thorin.bytes/get-uint32 msgBB 5)
                        length (Thorin.bytes/get-uint32 msgBB 9)]
                    (recur stateT))

                  #_:piece
                  (== msg-id 7)
                  (let [index (Thorin.bytes/get-uint32 msgBB 1)
                        begin (Thorin.bytes/get-uint32 msgBB 5)
                        blockBB (Thorin.bytes/buffer-slice msgBB 9 (- msg-length 9))]
                    (recur stateT))

                  #_:cancel
                  (and (== msg-id 8) (== msg-length 13))
                  (recur stateT)

                  #_:port
                  (and (== msg-id 9) (== msg-length 3))
                  (recur stateT)

                  #_:extended
                  (and (== msg-id 20))
                  (let [ext-msg-id (Thorin.bytes/get-uint8 msgBB 1)
                        payloadBB (Thorin.bytes/buffer-slice msgBB 2 (- msg-length 2))]
                    (cond

                      #_:handshake
                      (== ext-msg-id 0)
                      (let [data (-> (Thorin.bytes/to-byte-array payloadBB) (Thorin.bencode/decode) (keywordize-keys))]
                        (let  [ut-metadata-id (get-in data [:m :ut_metadata])
                               metadata_size (get data :metadata_size)]
                          #_(println :received-extened-handshake (:m data) metadata_size)
                          (cond

                            (not ut-metadata-id)
                            (throw (ex-info "extended handshake: no ut_metadata" data nil))

                            (not (number? metadata_size))
                            (throw (ex-info "extended handshake: metadata_size is not a number" data nil))

                            (not (< 0 metadata_size ut-metadata-max-size))
                            (throw (ex-info "extended handshake: metadata_size invalid size" data nil))

                            :else
                            (do
                              #_(println :sending-first-piece-request)
                              (>! send| (extended-msg ut-metadata-id {:msg_type 0
                                                                      :piece 0})))))
                        (recur (-> stateT
                                   (assoc! :peer-extended-data data)
                                   (assoc! :ut-metadata-max-rejects 2 #_(-> (/ metadata_size ut-metadata-block-size) (int) (+ 1))))))

                      (== ext-msg-id 3 #_(get-in stateT [:extensions "ut-metadata"]) #_(get-in stateT [:peer-extended-data "m" "ut_metadata"]))
                      (let [payload-str (Thorin.bytes/to-string payloadBB)
                            block-index (-> (clojure.string/index-of payload-str "ee") (+ 2))
                            data-str (subs payload-str 0 block-index)
                            data  (-> data-str (Thorin.bytes/to-byte-array) (Thorin.bencode/decode) (keywordize-keys))]
                        #_(println :ext-msg-id-3 data)
                        (condp == (:msg_type data)

                          #_:request
                          0
                          (let []
                            #_(println :request data)
                            (when-let [ut-metadata-id (get-in stateT [:peer-extended-data :m :ut_metadata])]
                              (>! send| (extended-msg ut-metadata-id {:msg_type 2
                                                                      :piece (:piece data)})))
                            (recur stateT))

                          #_:data
                          1
                          (let [blockBA (-> payloadBB
                                            (Thorin.bytes/buffer-slice block-index (- (Thorin.bytes/capacity payloadBB) block-index))
                                            (Thorin.bytes/to-byte-array))  #_(-> payload-str (subs block-index) (Thorin.bytes/to-byte-array))
                                ut-metadata-size (get-in stateT [:peer-extended-data :metadata_size])
                                downloaded (+ (:ut-metadata-downloaded stateT) (Thorin.bytes/alength blockBA))]
                            #_(println :got-piece data downloaded (Thorin.bytes/alength blockBA))
                            (cond
                              (== downloaded ut-metadata-size)
                              (let [metadataBA (Thorin.bytes/concat (persistent! (conj! (:ut-metadata-pieces stateT) blockBA)))
                                    metadata-hash (-> (Thorin.bytes/sha1 metadataBA) (Thorin.codec/hex-to-string))
                                    peer-infohash (-> (:peer-infohashBA stateT) (Thorin.codec/hex-to-string))]
                                (if-not (= metadata-hash peer-infohash)
                                  (throw (ex-info "metadata hash differs from peer's infohash" {} nil))
                                  (>! metadata| metadataBA))
                                (recur (-> stateT
                                           (assoc! :ut-metadata-downloaded 0)
                                           (assoc! :ut-metadata-pieces (transient [])))))

                              (> downloaded ut-metadata-size)
                              (let []
                                (throw (ex-info "downloaded metadata size is larger than declared" {:downloaded downloaded
                                                                                                    :ut-metadata-size ut-metadata-size} nil))
                                (recur (-> stateT
                                           (assoc! :ut-metadata-downloaded 0)
                                           (assoc! :ut-metadata-pieces (transient [])))))

                              :else
                              (let [ut-metadata-id (get-in stateT [:peer-extended-data :m :ut_metadata])
                                    downloaded-pieces (int (/ downloaded ut-metadata-block-size))
                                    next-piece downloaded-pieces]
                                #_(println :sending-next-piece-request next-piece downloaded-pieces downloaded)
                                (>! send| (extended-msg ut-metadata-id {:msg_type 0
                                                                        :piece next-piece}))
                                (recur (-> stateT
                                           (assoc! :ut-metadata-downloaded downloaded)
                                           (assoc! :ut-metadata-pieces (conj! (:ut-metadata-pieces stateT) blockBA)))))))

                          #_:reject
                          2
                          (let [ut-metadata-id (get-in stateT [:peer-extended-data :m :ut_metadata])]
                            #_(println :got-reject data)
                            (when (== 0 (:ut-metadata-max-rejects stateT))
                              (throw (ex-info "metadata request rejected" data nil)))
                            (>! send| (extended-msg ut-metadata-id {:msg_type 0
                                                                    :piece (:piece data)}))
                            (recur (-> stateT
                                       (assoc! :ut-metadata-max-rejects (dec (:ut-metadata-max-rejects stateT))))))

                          #_(println [:unsupported-ut-metadata-msg :ext-msg-id ext-msg-id])))

                      :else
                      (let []
                        #_(println [:unsupported-extension-msg :ext-msg-id ext-msg-id])
                        (recur stateT))))

                  :else
                  (let []
                    #_(println [:unknown-message :msg-id msg-id :msg-length msg-length])
                    (recur stateT)))))))

        (catch Exception ex (put! ex| ex)))
      (release))

    wire-protocol))



(comment

  clj -Sdeps '{:deps {org.clojure/clojure {:mvn/version "1.10.3"}
                      org.clojure/core.async {:mvn/version "1.3.618"}
                      expanse.bittorrent/bencode {:local/root "./bittorrent/bencode"}
                      expanse.bittorrent/wire {:local/root "./bittorrent/wire-protocol"}
                      expanse.bittorrent/spec {:local/root "./bittorrent/spec"}
                      expanse/bytes-jvm {:local/root "./expanse/bytes-jvm"}
                      expanse/codec-jvm {:local/root "./expanse/codec-jvm"}
                      expanse/core-jvm {:local/root "./expanse/core-jvm"}}}'
  
  clj -Sdeps '{:deps {org.clojure/clojurescript {:mvn/version "1.10.844"}
                      org.clojure/core.async {:mvn/version "1.3.618"}
                      expanse.bittorrent/bencode {:local/root "./bittorrent/bencode"}
                      expanse.bittorrent/wire {:local/root "./bittorrent/wire-protocol"}
                      expanse.bittorrent/spec {:local/root "./bittorrent/spec"}
                      expanse/bytes-js {:local/root "./expanse/bytes-js"}
                      expanse/codec-js {:local/root "./expanse/codec-js"}
                      expanse/bytes-meta {:local/root "./expanse/bytes-meta"}
                      expanse/core-js {:local/root "./expanse/core-js"}}}' \
   -M -m cljs.main --repl-env node --compile expanse.bittorrent.wire-protocol.core --repl
  
   (do
     (set! *warn-on-reflection* true)
     (set! *unchecked-math* true))
  
  (require
   '[clojure.core.async :as a :refer [chan go go-loop <! >!  take! put! offer! poll! alt! alts! close! onto-chan!
                                      pub sub unsub mult tap untap mix admix unmix pipe
                                      timeout to-chan  sliding-buffer dropping-buffer
                                      pipeline pipeline-async]]
   '[Thorin.bytes]
   '[Thorin.bencode]
   '[expanse.bittorrent.wire-protocol.core :as wire-protocol.core]
   :reload #_:reload-all)
  
  
  ;
  )

(comment


  (Thorin.bytes/get-uint32 (Thorin.bytes/buffer-wrap (Thorin.bytes/byte-array [0 0 0 5])) 0)
  (Thorin.bytes/get-uint32 (Thorin.bytes/buffer-wrap (Thorin.bytes/byte-array [0 0 1 3])) 0)


  ; The bit selected for the extension protocol is bit 20 from the right (counting starts at 0) . 
  ; So (reserved_byte [5] & 0x10) is the expression to use for checking if the client supports extended messaging
  (bit-and 2r00010000  0x10)
  ; => 16

  (->
   (Thorin.bytes/buffer-allocate 4)
   (Thorin.bytes/put-int 0 16384)
   (Thorin.bytes/get-int 0))

  (let [byte-buf  (Thorin.bytes/buffer-allocate 4)
        _ (Thorin.bytes/put-int byte-buf 0 16384)
        byte-arr (Thorin.bytes/to-byte-array byte-buf)]
    [(Thorin.bytes/alength byte-arr)
     (-> byte-arr
         (Thorin.bytes/buffer-wrap)
         (Thorin.bytes/get-uint32 0))])


  ;
  )


(comment

  (time
   (doseq [i (range 10000)
           j (range 10000)]
     (== i j)))
  ; "Elapsed time: 1230.363084 msecs"

  (time
   (doseq [i (range 10000)
           j (range 10000)]
     (= i j)))
  ; "Elapsed time: 3089.990067 msecs"

  ;
  )


(comment

  (do
    (time
     (let [kword :foo/bar]
       (dotimes [i 100000000]
         (= kword :foo/bar))))
    ; "Elapsed time: 191.077891 msecs"

    (time
     (let [kword :foo/bar]
       (dotimes [i 100000000]
         (identical? kword :foo/bar))))
    ; "Elapsed time: 96.919884 msecs"
    )


  ;
  )


(comment

  (do
    (time
     (let [x (atom (transient []))]
       (dotimes [i 10000000]
         (swap! x conj! i))
       (count (persistent! @x))))
    ;"Elapsed time: 684.808948 msecs"

    (time
     (let [x (volatile! (transient []))]
       (dotimes [i 10000000]
         (vswap! x conj! i))
       (count (persistent! @x))))
    ; "Elapsed time: 582.699983 msecs"

    (time
     (let [x (atom [])]
       (dotimes [i 10000000]
         (swap! x conj i))
       (count @x)))
    ; "Elapsed time: 1014.411053 msecs"

    (time
     (let [x (volatile! [])]
       (dotimes [i 10000000]
         (vswap! x conj i))
       (count @x)))
    ; "Elapsed time: 665.942603 msecs"
    )

  ;
  )



(comment
  
  (time
   (loop [i 10000000
          x (transient {})]
     (when (> i 0)
       
       (recur (dec i) (-> x
                          (assoc! :a 1)
                          (assoc! :b 2)
                          (assoc! :c 3))))))
  ; "Elapsed time: 577.725074 msecs"
  
  (time
   (loop [i 10000000
          x {}]
     (when (> i 0)
       (recur (dec i) (merge x {:a 1
                                :b 2
                                :c 3}) ))))
  ; "Elapsed time: 4727.433252 msecs"
  
  
  (time
   (loop [i 10000000
          x (transient {})]
     (when (> i 0)

       (recur (dec i) (-> (transient (persistent! x))
                          (assoc! :a 1)
                          (assoc! :b 2)
                          (assoc! :c 3))))))
  ; "Elapsed time: 2309.336101 msecs"
  
  
  ;
  )