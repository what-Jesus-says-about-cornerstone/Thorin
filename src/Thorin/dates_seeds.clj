#_(ns Thorin.datagram
    (:require
     [clojure.core.async :as a :refer [<! >! <!! >!! chan put! take! go alt! alts! do-alts close! timeout pipe mult tap untap
                                       pub sub unsub mix admix unmix dropping-buffer sliding-buffer pipeline pipeline-async to-chan! thread]]
     [clojure.string]
     [clojure.java.io :as io])
    (:import
     (java.net DatagramSocket InetSocketAddress DatagramPacket)))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defn process
  [opts]
  (let [{:keys [host port]} opts]
    (thread
      (let [socket (DatagramSocket. nil)]
        (.bind socket (InetSocketAddress. ^String host ^int port))))))
