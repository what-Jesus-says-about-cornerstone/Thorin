#_(ns Thorin.seed
  (:require
   [clojure.core.async :as a :refer [<! >! <!! >!! chan put! take! go alt! alts! do-alts close! timeout pipe mult tap untap
                                     pub sub unsub mix admix unmix dropping-buffer sliding-buffer pipeline pipeline-async to-chan! thread]]
   [clojure.string]
   [clojure.java.io :as io])
  (:import
   (java.util Random)))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defn random-bytes ^bytes
  [n]
  (let [byte-arr (byte-array n)]
    (-> (Random.) (.nextBytes byte-arr))
    byte-arr))