(ns Thorin.seed
  (:require
   [clojure.core.async :as a :refer [chan go go-loop <! >!  take! put! offer! poll! alt! alts! close! onto-chan!
                                     pub sub unsub mult tap untap mix admix unmix pipe
                                     timeout to-chan  sliding-buffer dropping-buffer
                                     pipeline pipeline-async]]
   [clojure.core.async.impl.protocols :refer [closed?]]
   [clojure.java.io :as io]

   [Thorin.bytes]
   [Thorin.codec]
   [Thorin.fs]
   [Thorin.transit]
   [cognitect.transit :as transit]))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defn now
  []
  (System/currentTimeMillis))

(defn gen-neighbor-id
  [target-idBA node-idBA]
  (->>
   [(Thorin.bytes/copy-byte-array target-idBA 0 10)
    (Thorin.bytes/copy-byte-array node-idBA 10 (Thorin.bytes/alength node-idBA))]
   (Thorin.bytes/concat)))

(defn encode-nodes
  [nodes]
  (->> nodes
       (map (fn [[id node]]
              (->>
               [(:idBA node)
                (->>
                 (clojure.string/split (:host node) #"\.")
                 (map #(Integer/parseInt %))
                 (Thorin.bytes/byte-array))
                (->
                 (doto
                  (Thorin.bytes/buffer-allocate 2)
                   (Thorin.bytes/put-uint16 0 (:port node)))
                 (Thorin.bytes/to-byte-array))]
               (Thorin.bytes/concat))))
       (Thorin.bytes/concat)))

(defn decode-nodes
  [nodesBA]
  (try
    (let [nodesBB (Thorin.bytes/buffer-wrap nodesBA)]
      (for [i (range 0 (Thorin.bytes/alength nodesBA) 26)]
        (let [idBA (Thorin.bytes/copy-byte-array nodesBA i (unchecked-add i 20))]
          {:id (Thorin.codec/hex-to-string idBA)
           :idBA idBA
           :host (str (Thorin.bytes/get-uint8 nodesBB (unchecked-add i 20)) "."
                      (Thorin.bytes/get-uint8 nodesBB (unchecked-add i 21)) "."
                      (Thorin.bytes/get-uint8 nodesBB (unchecked-add i 22)) "."
                      (Thorin.bytes/get-uint8 nodesBB (unchecked-add i 23)))
           :port (Thorin.bytes/get-uint16 nodesBB (unchecked-add i 24))})))
    (catch Exception ex nil)))


(defn decode-values
  [values]
  (->>
   (flatten [values])
   (sequence
    (comp
     (filter (fn [x] (Thorin.bytes/byte-array? x)))
     (map
      (fn [peer-infoBA]
        (let [peer-infoBB (Thorin.bytes/buffer-wrap  peer-infoBA)]
          {:host (str (Thorin.bytes/get-uint8 peer-infoBB 0) "."
                      (Thorin.bytes/get-uint8 peer-infoBB 1) "."
                      (Thorin.bytes/get-uint8 peer-infoBB 2) "."
                      (Thorin.bytes/get-uint8 peer-infoBB 3))
           :port (Thorin.bytes/get-uint16 peer-infoBB 4)})))))))

(defn decode-samples
  [samplesBA]
  (for [i (range 0 (Thorin.bytes/alength samplesBA) 20)]
    (Thorin.bytes/copy-byte-array samplesBA i (unchecked-add i 20))))

(defn xor-distance
  [xBA yBA]
  (let [xBA-length (Thorin.bytes/alength xBA)]
    (when-not (== xBA-length (Thorin.bytes/alength yBA))
      (throw (ex-info "xor-distance: args should have same length" {})))
    (let [resultBB (Thorin.bytes/buffer-allocate xBA-length)]
      (dotimes [i xBA-length]
        (Thorin.bytes/put-uint8 resultBB i (bit-xor (Thorin.bytes/aget-byte xBA i) (Thorin.bytes/aget-byte yBA i))))
      (Thorin.bytes/to-byte-array resultBB))))

(defn distance-compare
  [distance1BA distance2BA]
  (let [distance1BA-length (Thorin.bytes/alength distance1BA)]
    (when-not (== distance1BA-length (Thorin.bytes/alength distance2BA))
      (throw (ex-info "distance-compare: buffers should have same length" {})))
    (reduce
     (fn [result i]
       (let [a (Thorin.bytes/aget-byte distance1BA i)
             b (Thorin.bytes/aget-byte distance2BA i)]
         (cond
           (== a b) 0
           (< a b) (reduced -1)
           (> a b) (reduced 1))))
     0
     (range 0 distance1BA-length))))

(defn hash-key-distance-comparator-fn
  [targetBA]
  (fn [id1 id2]
    (distance-compare
     (xor-distance targetBA (Thorin.codec/hex-to-bytes id1))
     (xor-distance targetBA (Thorin.codec/hex-to-bytes id2)))))

(defn sorted-map-buffer
  "sliding according to comparator sorted-map buffer"
  [n comparator]
  (let [collA (atom (sorted-map-by comparator))]
    (reify
      clojure.core.async.impl.protocols/UnblockingBuffer
      clojure.core.async.impl.protocols/Buffer
      (full? [this] false)
      (remove! [this]
        (let [[id node :as item] (first @collA)]
          (swap! collA dissoc id)
          item))
      (add!* [this [id node]]
        (swap! collA assoc id node)
        (when (> (count @collA) n)
          (swap! collA dissoc (key (last @collA))))
        this)
      (close-buf! [this])
      clojure.lang.Counted
      (count [this] (count @collA)))))


(def transit-write
  (let [handlers {Thorin.bytes/ByteArray
                  (transit/write-handler
                   (fn [byte-arr] "::Thorin.bytes/byte-array")
                   (fn [byte-arr] (Thorin.codec/hex-to-string byte-arr)))
                  clojure.core.async.impl.channels.ManyToManyChannel
                  (transit/write-handler
                   (fn [c|] "ManyToManyChannel")
                   (fn [c|] nil))}]
    (fn [data]
      (Thorin.transit/write-to-string data :json-verbose {:handlers handlers}))))

(def transit-read
  (let [handlers {"::Thorin.bytes/byte-array"
                  (transit/read-handler
                   (fn [string] (Thorin.codec/hex-to-bytes string)))
                  "ManyToManyChannel"
                  (transit/read-handler
                   (fn [string] nil))}]
    (fn [data-string]
      (Thorin.transit/read-string data-string :json-verbose {:handlers handlers}))))

(defn read-state-file
  [filepath]
  (go
    (try
      (when (Thorin.fs/path-exists? filepath)
        (let [data-string (Thorin.bytes/to-string (Thorin.fs/read-file filepath))]
          (transit-read data-string)))
      (catch Exception ex (println ::read-state-file ex)))))

(defn write-state-file
  [filepath data]
  (go
    (try
      (let [data-string (transit-write data)]
        (Thorin.fs/make-parents filepath)
        (Thorin.fs/write-file filepath data-string))
      (catch Exception ex (println :write-state-file ex)))))

(defn send-krpc-request-fn
  [{:as opts
    :keys [msg|mult
           send|]}]
  (let [requestsA (atom {})
        msg|tap (tap msg|mult (chan (sliding-buffer 512)))]
    (go
      (loop []
        (when-let [{:keys [msg] :as value} (<! msg|tap)]
          (when-let [txn-id (some-> (:t msg) (Thorin.codec/hex-to-string))]
            (when-let [response| (get @requestsA txn-id)]
              (put! response| value)
              (close! response|)
              (swap! requestsA dissoc txn-id)))
          (recur))))
    (fn send-krpc-request
      ([msg node]
       (send-krpc-request msg node (timeout 2000)))
      ([msg {:keys [host port]} timeout|]
       (let [txn-id (Thorin.codec/hex-to-string (:t msg))
             response| (chan 1)]
         (put! send| {:msg msg
                      :host host
                      :port port})
         (swap! requestsA assoc txn-id response|)
         (take! timeout| (fn [_]
                           (when-not (closed? response|)
                             (close! response|)
                             (swap! requestsA dissoc txn-id))))
         response|)))))

(defn chan-buf
  [^clojure.core.async.impl.channels.ManyToManyChannel c|]
  (.-buf c|))

(defn fixed-buf-size
  [^clojure.core.async.impl.channels.ManyToManyChannel c|]
  (.-n ^clojure.core.async.impl.buffers.FixedBuffer (.-buf c|)))


(comment

  (do
    (defn hash-string
      [letter]
      (clojure.string/join "" (take 40 (repeatedly (constantly letter)))))

    (def targetB (js/Buffer.from (hash-string "5")  "hex"))

    (def sm (sorted-map-by (Thorin.seed/hash-key-distance-comparator-fn targetB)))

    (def sm (->
             (reduce
              (fn [result letter]
                (assoc result (hash-string letter) letter))
              (sorted-map-by (Thorin.seed/hash-key-distance-comparator-fn targetB))
              (shuffle ["0" "1" "2" "3" "4" "5" "6" "7" "8" "9" "a" "b" "c" "d" "e" "f"]))
             (assoc (hash-string "2") "2")))



    (println (take 16 (vals sm))))

  ;
  )

(comment

  (.-length (js/Buffer.from (hash-string "5")   "hex"))

  (def targetB (js/Buffer.from (hash-string "5")  "hex"))

  (.toString (xor-distance targetB (js/Buffer.from (hash-string "4")  "hex")) "hex")
  (.toString (xor-distance targetB (js/Buffer.from (hash-string "c")  "hex")) "hex")
  (.toString (xor-distance targetB (js/Buffer.from (hash-string "5")  "hex")) "hex")
  (.toString (xor-distance targetB (js/Buffer.from (hash-string "d")  "hex")) "hex")

  (js/Array.from (js/Buffer.from (hash-string "6")  "hex"))
  (js/Array.from (js/Buffer.from (hash-string "5")  "hex"))
  (js/Array.from (js/Buffer.from (hash-string "c")  "hex"))

  (js/Array.from (js/Buffer.from (hash-string "8")  "hex"))

  ;
  )

(comment

  (extend-protocol IPrintWithWriter
    js/Buffer
    (-pr-writer [buffer writer _]
      (write-all writer "#js/buffer \"" (.toString buffer) "\"")))

  (cljs.reader/register-tag-parser!
   'js/buffer
   (fn [value]
     (js/Buffer.from value)))

  (cljs.reader/read-string

   "#js/buffer \"96190f486de62449099f9caf852964b2e12058dd\"")

  (println (cljs.reader/read-string {:readers {'foo identity}} "#foo :asdf"))

  ;
  )



(comment

  (time
   (let [byte-arr (Thorin.bytes/byte-array 20)]
     (dotimes [i 100000]
       (let [x (mod i 20)]
         (aget byte-arr x)
         (Thorin.bytes/aset-uint8 byte-arr x x)))
     (vec byte-arr)))

  ; jvm    "Elapsed time: 1124.560132 msecs"
  ; nodejs "Elapsed time: 8.175314 msecs"
  ; aget is the reason - without it it's 14.898965 msecs on jvm and 3.294965 msecs on nodejs


  (time
   (let [buffer (Thorin.bytes/buffer-allocate 20)]
     (dotimes [i 10000000]
       (let [x (mod i 20) #_(unchecked-remainder-int i 20)]
         (Thorin.bytes/get-byte buffer x)
         (Thorin.bytes/put-byte buffer x x)))
     (vec (Thorin.bytes/to-byte-array buffer))))

  ; jvm    "Elapsed time: 122.298044 msecs"
  ; nodejs "Elapsed time: 82.160827 msecs"
  ; jvm unchecked-remainder-int "Elapsed time: 73.329539 msecs"

  ; aget needs type hint ^bytes
  (time
   (let [^bytes byte-arr (Thorin.bytes/byte-array 20)]
     (dotimes [i 100000]
       (let [^int x (mod i 20)]
         (aget byte-arr x)
         (Thorin.bytes/aset-uint8 byte-arr x x)))
     (vec byte-arr)))


  (time
   (let [byte-arr (Thorin.bytes/byte-array 20)]
     (dotimes [i 10000000]
       (let [x (mod i 20)]
         (Thorin.bytes/aget-byte byte-arr x)
         (Thorin.bytes/aset-uint8 byte-arr x x)))
     (vec byte-arr)))

  ; jvm    "Elapsed time: 704.516302 msecs"
  ; nodejs "Elapsed time: 49.059405 msecs"


  (time
   (let [^bytes byte-arr (Thorin.bytes/byte-array 20)]
     (dotimes [i 10000000]
       (let [x (unchecked-remainder-int i 20)]
         (aget byte-arr x)
         (aset-byte byte-arr x (unchecked-byte x))))
     (vec byte-arr)))

  ; "Elapsed time: 655.999327 msecs"

  (time
   (let [byte-arr (Thorin.bytes/byte-array 20)]
     (dotimes [i 100000000]
       (Thorin.bytes/alength byte-arr))))

  ; jvm "Elapsed time: 51.61525 msecs"
  ; nodejs "Elapsed time: 139.426112 msecs"

  (time
   (let [ba (Thorin.bytes/byte-array 20)
         foo (fn []
               (Thorin.bytes/alength ba))]
     (dotimes [i 10000000]
       (unchecked-add i (foo))
       #_(+ i (foo)))))

  ; +             "Elapsed time: 58.809468 msecs"
  ; unchecked-add "Elapsed time: 15.03717 msecs"

  (time
   (let [node {:host "11.11.11.11"}]
     (dotimes [i 1000000]
       (clojure.string/split (:host node) #"\."))))

  ; jvm    "Elapsed time: 478.691944 msecs"
  ; nodejs "Elapsed time: 1762.992872 msecs"


  (time
   (dotimes [i 1000000]
     (str 1 "."
          2 "."
          3 "."
          4 ".")))

  ; jvm    "Elapsed time: 663.926413 msecs"
  ; nodejs "Elapsed time: 654.183687 msecs"


  (time
   (dotimes [i 1000000]
     (clojure.string/join "." [1 2 3 4])))

  ; jvm "Elapsed time: 461.957236 msecs"
  ; nodejs "Elapsed time: 1087.789923 msecs"

  (time
   (let [foo (fn [] 1)]
     (dotimes [i 1000000]
       (str (foo) "."
            (foo) "."
            (foo) "."
            (foo) "."))))
  ; jvm "Elapsed time: 672.591089 msecs"
  ; nodejs "Elapsed time: 711.664806 msecs"



  (time
   (dotimes [i 1000000]
     (let [bb (Thorin.bytes/buffer-allocate 20)]
       (dotimes [i 20]
         (Thorin.bytes/put-uint8 bb i 8))
       (Thorin.bytes/to-byte-array bb))))

  ; "Elapsed time: 250.540999 msecs"

  (time
   (dotimes [i 1000000]
     (let [ba (Thorin.bytes/byte-array 20)]
       (dotimes [i 20]
         (Thorin.bytes/aset-uint8 ba i 8))
       ba)))

  ; "Elapsed time: 1281.031404 msecs"
  ; 


  (time
   (let [bb (Thorin.bytes/buffer-allocate 20)]
     (dotimes [i 100000000]
       (Thorin.bytes/put-uint8 bb 8 8)
       (Thorin.bytes/get-uint8 bb 8))))
  (time
   (let [^bytes ba (Thorin.bytes/byte-array 20)]
     (dotimes [i 100000000]
       (Thorin.bytes/aset-uint8 ba 8 8)
       (Thorin.bytes/aget-byte ba 8))))

  ; bb no put "Elapsed time: 56.157778 msecs"
  ; bb with put "Elapsed time: 59.037743 msecs"
  ; ba no set "Elapsed time: 54.943259 msecs"
  ; ba with set "Elapsed time: 6334.917527 msecs"

  (time (dotimes [i 10000]
          (let [x {}]
            (dotimes [i 10000]
              (identity x)))))
  ; "Elapsed time: 51.508801 msecs"

  (time (dotimes [i 10000]
          (let [x {}]
            (doseq [i (range 0 10000)]
              (identity x)))))
  ; "Elapsed time: 354.270582 msecs"

  (time (dotimes [i 10000]
          (reduce
           (fn [r x]
             (identity r))
           {}
           (range 0 10000))))
  ; "Elapsed time: 898.962197 msecs"


  (time
   (dotimes [i 1000000]
     (Thorin.codec/hex-to-bytes "197957dab1d2900c5f6d9178656d525e22e63300")))
  ; "Elapsed time: 93.882265 msecs"

  (time
   (let [ba (Thorin.codec/hex-to-bytes "197957dab1d2900c5f6d9178656d525e22e63300")]
     (dotimes [i 1000000]
       (Thorin.codec/hex-to-string ba))))
  ; "Elapsed time: 102.516529 msecs"

  ;
  )
