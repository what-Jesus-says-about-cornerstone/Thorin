#_(ns Thorin.bencode
    (:require
     [clojure.core.async :as a :refer [<! >! <!! >!! chan put! take! go alt! alts! do-alts close! timeout pipe mult tap untap
                                       pub sub unsub mix admix unmix dropping-buffer sliding-buffer pipeline pipeline-async to-chan! thread]]
     [clojure.string]
     [clojure.java.io :as io]
     [clojure.walk]
     [Thorin.seed])
    (:import
     (java.io ByteArrayOutputStream ByteArrayInputStream PushbackInputStream)
     (org.apache.commons.codec.binary Hex)))

(def i-int (int \i))
(def e-int (int \e))
(def d-int (int \d))
(def l-int (int \l))
(def colon-int (int \:))

(defmulti encode*
  (fn encode*-dispatch-fn
    ([value baos]
     (cond
       (bytes? value) :byte-arr
       (string? value) :string
       (map? value) :dictionary
       (sequential? value) :list
       (int? value) :integer))
    ([value baos dispatch-value] dispatch-value)))

(defmethod encode* :byte-arr
  [^bytes value ^ByteArrayOutputStream baos & args]
  (.writeBytes baos (-> (alength value) (str) (.getBytes "UTF-8")))
  (.write baos colon-int)
  (.writeBytes baos value))

(defmethod encode* :string
  [^String value ^ByteArrayOutputStream baos & args]
  (encode* (.getBytes value "UTF-8") baos :byte-arr))

(defmethod encode* :integer
  [^Integer value ^ByteArrayOutputStream baos & args]
  (.write baos i-int)
  (.writeBytes baos (-> value (str) (.getBytes "UTF-8")))
  (.write baos e-int))

(defmethod encode* :dictionary
  [value ^ByteArrayOutputStream baos & args]
  (.write baos d-int)
  (doseq [[k v] value]
    (encode* (str k) baos)
    (encode* v baos))
  (.write baos e-int))

(defmethod encode* :list
  [value ^ByteArrayOutputStream baos & args]
  (.write baos l-int)
  (doseq [v value]
    (encode* v baos))
  (.write baos e-int))

(defn encode ^bytes
  [data]
  (let [baos (ByteArrayOutputStream.)]
    (encode* data baos)
    (.toByteArray baos)))

(defn read-until ^bytes
  [^PushbackInputStream pbis ^Integer target-byte]
  (let [baos (ByteArrayOutputStream.)]
    (loop [byte (.read pbis)]
      (cond
        (== byte target-byte)
        (let []
          (.unread pbis byte)
          (.toByteArray baos))

        :else
        (let []
          (.write baos byte)
          (recur (.read pbis)))))))

(defmulti decode*
  (fn decode*-dispatch-fn
    ([^PushbackInputStream pbis]
     (let [byte (.read pbis)]
       (.unread pbis byte)
       (condp == byte
         -1 (throw (ex-info "input stream ends unexpectedly" {}))
         d-int :dictionary
         i-int :integer
         l-int :list
         :else :byte-arr)))
    ([pbis dispatch-value] dispatch-value)))

(defmethod decode* :byte-arr ^bytes
  [^PushbackInputStream pbis & args]
  (let [sizeBA (read-until pbis colon-int)
        size (Integer/parseInt (String. sizeBA "UTF-8"))
        _ (.read pbis)
        dataBA (.readNBytes pbis size)]
    dataBA))

(defmethod decode* :string ^String
  [^PushbackInputStream pbis & args]
  (->
   (decode* pbis :byte-arr)
   (String. "UTF-8")))

(defmethod decode* :integer ^Integer
  [^PushbackInputStream pbis & args]
  (.read pbis)
  (let [byte-arr (read-until pbis e-int)]
    (.read pbis)
    (Integer/parseInt (String. byte-arr "UTF-8"))))

(defmethod decode* :dictionary
  [^PushbackInputStream pbis & args]
  (let []
    (.read pbis)
    (loop [resultT (transient [])]
      (let [byte (.read pbis)]
        (.unread pbis byte)
        (cond

          (== byte e-int)
          (let [result (apply array-map (persistent! resultT))]
            (.read pbis)
            result)

          (even? (count resultT))
          (recur (conj! resultT (decode* pbis :string)))

          (== byte i-int)
          (recur (conj! resultT (decode* pbis :integer)))

          (== byte d-int)
          (recur (conj! resultT (decode* pbis :dictionary)))

          (== byte l-int)
          (recur (conj! resultT (decode* pbis :list)))

          :else
          (recur (conj! resultT (decode* pbis :byte-arr))))))))

(defmethod decode* :list
  [^PushbackInputStream pbis & args]
  (let []
    (.read pbis)
    (loop [resultT (transient [])]
      (let [byte (.read pbis)]
        (.unread pbis byte)
        (cond

          (== byte e-int)
          (let []
            (.read pbis)
            (persistent! resultT))

          (== byte i-int)
          (recur (conj! resultT (decode* pbis :integer)))

          (== byte d-int)
          (recur (conj! resultT (decode* pbis :dictionary)))

          (== byte l-int)
          (recur (conj! resultT (decode* pbis :list)))

          :else
          (recur (conj! resultT (decode* pbis :byte-arr))))))))

(defn decode
  [^bytes byte-arr]
  (let [pbis (->
              (ByteArrayInputStream. byte-arr)
              (PushbackInputStream.))]
    (decode* pbis)))



(comment

  (in-ns 'Thorin.bencode)

  (Thorin.main/reload)

  (Hex/encodeHexString (Thorin.seed/random-bytes 20))

  (let [txn-id (Hex/decodeHex "aabb")
        id (Hex/decodeHex "f6212aa693453ce52ffa51f41457b2c5633a045c")
        msg {:t txn-id
             :r {:id id :a 1}
             :b 2
             :list [2 {:c 4}]}
        encoded (-> msg
                    (clojure.walk/stringify-keys)
                    (encode))
        _ (println (String. encoded "UTF-8"))
        decoded (-> encoded
                    (decode)
                    (clojure.walk/keywordize-keys))
        walk-bytes-to-hex (fn [data] (clojure.walk/postwalk (fn [form] (if (bytes? form) (Hex/encodeHexString form) form)) data))]
    (println (walk-bytes-to-hex decoded))
    (println (walk-bytes-to-hex msg))))