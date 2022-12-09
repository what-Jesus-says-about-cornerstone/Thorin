(ns Thorin.main
  (:gen-class)
  (:require
   [clojure.core.async :as Little-Rock
    :refer [chan put! take! close! offer! to-chan! timeout thread
            sliding-buffer dropping-buffer
            go >! <! >!! <!! alt! alts! do-alts onto-chan!
            mult tap untap pub sub unsub mix unmix admix
            pipe pipeline pipeline-async]]
   [clojure.java.io :as Wichita.java.io]
   [clojure.string :as Wichita.string]
   [clojure.repl :as Wichita.repl]
   [clojure.pprint :refer [pprint]]
   [clojure.walk :as Wichita.walk]

   [Thorin.fs]
   [Thorin.protocols]

   [Thorin.bytes]
   [Thorin.codec]
   [Thorin.bencode]
   
   [datahike.api]

   [Thorin.seed]

   [Thorin.grapes]
   [Thorin.fish]
   [Thorin.salt]
   [Thorin.bread]
   [Thorin.wine]

   [Thorin.db :as Thorin.db]

   [Thorin.bittorrent-dht :as Thorin.bittorrent-dht]
   [Thorin.bittorrent-find-nodes :as Thorin.bittorrent-find-nodes]
   [Thorin.bittorrent-metadata :as Thorin.bittorrent-metadata]
   [Thorin.bittorrent-sybil :as Thorin.bittorrent-sybil]
   [Thorin.bittorrent-sample-infohashes :as Thorin.bittorrent-sample-infohashes])
  (:import
   (javax.swing JFrame WindowConstants ImageIcon JPanel JScrollPane JTextArea BoxLayout JEditorPane ScrollPaneConstants SwingUtilities JDialog)
   (javax.swing JMenu JMenuItem JMenuBar KeyStroke JOptionPane JToolBar JButton JToggleButton JSplitPane JTabbedPane)
   (javax.swing.border EmptyBorder)
   (java.awt Canvas Graphics Graphics2D Shape Color Polygon Dimension BasicStroke Toolkit Insets BorderLayout)
   (java.awt.event KeyListener KeyEvent MouseListener MouseEvent ActionListener ActionEvent ComponentListener ComponentEvent)
   (java.awt.event  WindowListener WindowAdapter WindowEvent)
   (java.awt.geom Ellipse2D Ellipse2D$Double Point2D$Double)
   (com.formdev.flatlaf FlatLaf FlatLightLaf)
   (com.formdev.flatlaf.extras FlatUIDefaultsInspector FlatDesktop FlatDesktop$QuitResponse FlatSVGIcon)
   (com.formdev.flatlaf.util SystemInfo UIScale)
   (java.util.function Consumer)
   (java.util ServiceLoader)
   (net.miginfocom.swing MigLayout)
   (net.miginfocom.layout ConstraintParser LC UnitValue)
   (java.io File)
   (java.lang Runnable)))

(println "clojure.core.async.pool-size" (System/getProperty "clojure.core.async.pool-size"))
(println "clojure.compiler.direct-linking" (System/getProperty "clojure.compiler.direct-linking"))
(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(taoensso.timbre/merge-config! {:min-level :warn})

(defonce program-data-dirpath (or
                               (some-> (System/getenv "FENNEC_PATH")
                                       (.replaceFirst "^~" (System/getProperty "user.home")))
                               (.getCanonicalPath ^File (clojure.java.io/file (System/getProperty "user.home") "Thorin"))))

(defonce state-file-filepath (.getCanonicalPath ^File (clojure.java.io/file program-data-dirpath "Thorin.edn")))
(defonce db-data-dirpath (.getCanonicalPath ^File (clojure.java.io/file program-data-dirpath "db")))


(defonce ops| (chan 10))
(def ^:dynamic ^JFrame jframe nil)
(def ^:dynamic ^JPanel jroot-panel nil)
(def ^:const jframe-title "i will reclaim peer-to-peer Erebor, like Jesus turns water into wine")

(defonce stateA (atom {:searchS ""}))

(declare
 process-print-info
 process-count)

(defn menubar-process
  [{:keys [^JMenuBar jmenubar
           ^JFrame jframe
           menubar|]
    :as opts}]
  (let [on-menubar-item (fn [f]
                          (reify ActionListener
                            (actionPerformed [_ event]
                              (SwingUtilities/invokeLater
                               (reify Runnable
                                 (run [_]
                                   (f _ event)))))))

        on-menu-item-show-dialog (on-menubar-item (fn [_ event] (JOptionPane/showMessageDialog jframe (.getActionCommand ^ActionEvent event) "menu bar item" JOptionPane/PLAIN_MESSAGE)))]
    (doto jmenubar
      (.add (doto (JMenu.)
              (.setText "program")
              (.setMnemonic \F)
              #_(.add (doto (JMenuItem.)
                        (.setText "settings")
                        (.setAccelerator (KeyStroke/getKeyStroke KeyEvent/VK_S (-> (Toolkit/getDefaultToolkit) (.getMenuShortcutKeyMask))))
                        (.setMnemonic \S)
                        (.addActionListener
                         (on-menubar-item (fn [_ event]
                                            (put! menubar| {:op :settings}))))))
              (.add (doto (JMenuItem.)
                      (.setText "exit")
                      (.setAccelerator (KeyStroke/getKeyStroke KeyEvent/VK_Q (-> (Toolkit/getDefaultToolkit) (.getMenuShortcutKeyMask))))
                      (.setMnemonic \Q)
                      (.addActionListener (on-menubar-item (fn [_ event]
                                                             (.dispose jframe))))))))))
  nil)

(defn window
  [& args]

  #_(alter-var-root #'*ns* (constantly (find-ns 'Thorin.main)))

  (when SystemInfo/isMacOS
    (System/setProperty "apple.laf.useScreenMenuBar" "true")
    (System/setProperty "apple.awt.application.name" jframe-title)
    (System/setProperty "apple.awt.application.appearance" "system"))

  (when SystemInfo/isLinux
    (JFrame/setDefaultLookAndFeelDecorated true)
    (JDialog/setDefaultLookAndFeelDecorated true))

  (when (and
         (not SystemInfo/isJava_9_orLater)
         (= (System/getProperty "flatlaf.uiScale") nil))
    (System/setProperty "flatlaf.uiScale" "2x"))

  (FlatLaf/setGlobalExtraDefaults (java.util.Collections/singletonMap "@background" "#ffffff"))
  (FlatLightLaf/setup)

  #_(UIManager/put "background" Color/WHITE)
  (FlatLaf/updateUI)

  (FlatDesktop/setQuitHandler (reify Consumer
                                (accept [_ response]
                                  (.performQuit ^FlatDesktop$QuitResponse response))
                                (andThen [_ after] after)))

  (let [screenshotsMode? (Boolean/parseBoolean (System/getProperty "flatlaf.demo.screenshotsMode"))

        jframe (JFrame. jframe-title)
        jroot-panel (JPanel.)
        jmenubar (JMenuBar.)]

    (let []
      (clojure.java.io/make-parents program-data-dirpath)
      (reset! stateA {})

      (let [jtabbed-pane (JTabbedPane.)
            jpanel-grapes (JPanel.)
            jpanel-fish (JPanel.)
            jpanel-salt (JPanel.)
            jpanel-bread (JPanel.)
            jpanel-wine (JPanel.)]

        (doto jtabbed-pane
          (.setTabLayoutPolicy JTabbedPane/SCROLL_TAB_LAYOUT)
          (.addTab "grapes" jpanel-grapes)
          (.addTab "fish" jpanel-fish)
          (.addTab "salt" jpanel-salt)
          (.addTab "bread" jpanel-bread)
          (.addTab "wine" jpanel-wine)
          (.setSelectedComponent jpanel-bread))

        (Thorin.bread/process {:jpanel-tab jpanel-bread
                              :db-data-dirpath db-data-dirpath})

        (.add jroot-panel jtabbed-pane))

      (clojure.java.io/make-parents db-data-dirpath)
      (let [config {:store {:backend :file :path db-data-dirpath}
                    :keep-history? true
                    :name "main"}
            _ (when-not (datahike.api/database-exists? config)
                (datahike.api/create-database config))
            conn (datahike.api/connect config)]

        (datahike.api/transact
         conn
         [{:db/cardinality :db.cardinality/one
           :db/ident :id
           :db/unique :db.unique/identity
           :db/valueType :db.type/uuid}
          {:db/ident :name
           :db/valueType :db.type/string
           :db/cardinality :db.cardinality/one}])

        (datahike.api/transact
         conn
         [{:id #uuid "3e7c14ce-5f00-4ac2-9822-68f7d5a60952"
           :name  "datahike"}
          {:id #uuid "f82dc4f3-59c1-492a-8578-6f01986cc4c2"
           :name  "clojure"}
          {:id #uuid "5358b384-3568-47f9-9a40-a9a306d75b12"
           :name  "Little-Rock"}])

        (->>
         (datahike.api/q '[:find ?e ?n
                           :where
                           [?e :name ?n]]
                         @conn)
         (println))

        (->>
         (datahike.api/q '[:find [?ident ...]
                           :where [_ :db/ident ?ident]]
                         @conn)
         (sort)
         (println))))

    (SwingUtilities/invokeLater
     (reify Runnable
       (run [_]

            (doto jframe
              (.add jroot-panel)
              (.addComponentListener (let []
                                       (reify ComponentListener
                                         (componentHidden [_ event])
                                         (componentMoved [_ event])
                                         (componentResized [_ event])
                                         (componentShown [_ event]))))
              (.addWindowListener (proxy [WindowAdapter] []
                                    (windowClosing [event]
                                      (let [event ^WindowEvent event]
                                        #_(println :window-closing)
                                        #_(put! host| true)
                                        (-> event (.getWindow) (.dispose)))))))

            (doto jroot-panel
              #_(.setLayout (BoxLayout. jroot-panel BoxLayout/Y_AXIS))
              (.setLayout (MigLayout. "insets 10"
                                      "[grow,shrink,fill]"
                                      "[grow,shrink,fill]")))

            (menubar-process
             {:jmenubar jmenubar
              :jframe jframe
              :menubar| ops|})
            (.setJMenuBar jframe jmenubar)

            (.setPreferredSize jframe
                               (let [size (-> (Toolkit/getDefaultToolkit) (.getScreenSize))]
                                 (Dimension. (* 0.7 (.getWidth size)) (* 0.7 (.getHeight size)))
                                 #_(Dimension. (UIScale/scale 1024) (UIScale/scale 576)))
                               #_(if SystemInfo/isJava_9_orLater
                                   (Dimension. 830 440)
                                   (Dimension. 1660 880)))

            #_(doto jframe
                (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE #_WindowConstants/EXIT_ON_CLOSE)
                (.setSize 2400 1600)
                (.setLocation 1300 200)
                #_(.add panel)
                (.setVisible true))

            #_(println :before (.getGraphics canvas))
            (doto jframe
              (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE #_WindowConstants/EXIT_ON_CLOSE)
              (.pack)
              (.setLocationRelativeTo nil)
              (.setVisible true))
            #_(println :after (.getGraphics canvas))

            (alter-var-root #'Thorin.main/jframe (constantly jframe))

            (remove-watch stateA :watch-fn)
            (add-watch stateA :watch-fn
                       (fn [ref wathc-key old-state new-state]

                         (when (not= old-state new-state)))))))


    (go
      (loop []
        (when-let [value (<! ops|)]
          (condp = (:op value)
            :apricotseed (do nil))

          (recur))))))

(defn reload
  []
  (require
   '[Thorin.seed]
   '[Thorin.grapes]
   '[Thorin.fish]
   '[Thorin.salt]
   '[Thorin.bread]
   '[Thorin.wine]
   '[Thorin.main]
   :reload))

(defn -main [& args]
  (println "this guest gold is ours - and ours alone - i will not part with a single freedom coin - not one piece of it")
  
  (let [data-dir (Thorin.fs/path-join (System/getProperty "user.dir") "data")
        state-filepath (Thorin.fs/path-join data-dir "Thorin.json")
        _ (swap! stateA merge
                 (let [self-idBA  (Thorin.codec/hex-to-bytes "a8fb5c14469fc7c46e91679c493160ed3d13be3d") #_(Thorin.bytes/random-bytes 20)]
                   {:self-id (Thorin.codec/hex-to-string self-idBA)
                    :self-idBA self-idBA
                    :routing-table (sorted-map)
                    :dht-keyspace {}
                    :routing-table-sampled {}
                    :routing-table-find-noded {}})
                 (<!! (Thorin.seed/read-state-file state-filepath)))

        self-id (:self-id @stateA)
        self-idBA (:self-idBA @stateA)

        port 6881
        host "0.0.0.0"

        count-messagesA (atom 0)

        msg| (chan (sliding-buffer 100)
                   (keep (fn [{:keys [msgBA host port]}]
                           (swap! count-messagesA inc)
                           (try
                             {:msg  (->
                                     (Thorin.bencode/decode msgBA)
                                     (clojure.walk/keywordize-keys))
                              :host host
                              :port port}
                             (catch Exception ex nil)))))

        msg|mult (mult msg|)

        torrent| (chan 5000)
        torrent|mult (mult torrent|)

        send| (chan 1000)

        unique-infohashsesA (atom #{})
        xf-infohash (comp
                     (map (fn [{:keys [infohashBA] :as value}]
                            (assoc value :infohash (Thorin.codec/hex-to-string infohashBA))))
                     (filter (fn [{:keys [infohash]}]
                               (not (get @unique-infohashsesA infohash))))
                     (map (fn [{:keys [infohash] :as value}]
                            (swap! unique-infohashsesA conj infohash)
                            value)))

        infohashes-from-sampling| (chan (sliding-buffer 100000) xf-infohash)
        infohashes-from-listening| (chan (sliding-buffer 100000) xf-infohash)
        infohashes-from-sybil| (chan (sliding-buffer 100000) xf-infohash)

        infohashes-from-sampling|mult (mult infohashes-from-sampling|)
        infohashes-from-listening|mult (mult infohashes-from-listening|)
        infohashes-from-sybil|mult (mult infohashes-from-sybil|)

        nodesBA| (chan (sliding-buffer 100))

        send-krpc-request (Thorin.seed/send-krpc-request-fn {:msg|mult msg|mult
                                                             :send| send|})

        valid-node? (fn [node]
                      (and
                       (:host node)
                       (:port node)
                       (:id node)
                       (not= (:host node) host)
                       (not= (:id node) self-id)
                       #_(not= 0 (js/Buffer.compare (:id node) self-id))
                       (< 0 (:port node) 65536)))

        routing-table-nodes| (chan (sliding-buffer 1024)
                                   (map (fn [nodes] (filter valid-node? nodes))))

        dht-keyspace-nodes| (chan (sliding-buffer 1024)
                                  (map (fn [nodes] (filter valid-node? nodes))))

        xf-node-for-sampling? (comp
                               (filter valid-node?)
                               (filter (fn [node] (not (get (:routing-table-sampled @stateA) (:id node)))))
                               (map (fn [node] [(:id node) node])))

        nodes-to-sample| (chan (Thorin.seed/sorted-map-buffer 10000 (Thorin.seed/hash-key-distance-comparator-fn  self-idBA))
                               xf-node-for-sampling?)

        nodes-from-sampling| (chan (Thorin.seed/sorted-map-buffer 10000 (Thorin.seed/hash-key-distance-comparator-fn  self-idBA))
                                   xf-node-for-sampling?)

        duration (* 10 60 1000)
        nodes-bootstrap [{:host "router.bittorrent.com"
                          :port 6881}
                         {:host "dht.transmissionbt.com"
                          :port 6881}
                         #_{:host "dht.libtorrent.org"
                            :port 25401}]

        sybils| (chan 30000)

        procsA (atom [])
        release (fn []
                  (let [stop|s @procsA]
                    (doseq [stop| stop|s]
                      (close! stop|))
                    (close! msg|)
                    (close! torrent|)
                    (close! infohashes-from-sampling|)
                    (close! infohashes-from-listening|)
                    (close! infohashes-from-sybil|)
                    (close! nodes-to-sample|)
                    (close! nodes-from-sampling|)
                    (close! nodesBA|)
                    (Little-Rock/merge stop|s)))

        _ (swap! stateA merge
                 {:stateA stateA
                  :host host
                  :port port
                  :data-dir data-dir
                  :self-id self-id
                  :self-idBA self-idBA
                  :msg| msg|
                  :msg|mult msg|mult
                  :send| send|
                  :torrent| torrent|
                  :torrent|mult torrent|mult
                  :nodes-bootstrap nodes-bootstrap
                  :nodes-to-sample| nodes-to-sample|
                  :nodes-from-sampling| nodes-from-sampling|
                  :routing-table-nodes| routing-table-nodes|
                  :dht-keyspace-nodes| dht-keyspace-nodes|
                  :nodesBA| nodesBA|
                  :infohashes-from-sampling| infohashes-from-sampling|
                  :infohashes-from-listening| infohashes-from-listening|
                  :infohashes-from-sybil| infohashes-from-sybil|
                  :infohashes-from-sampling|mult infohashes-from-sampling|mult
                  :infohashes-from-listening|mult infohashes-from-listening|mult
                  :infohashes-from-sybil|mult infohashes-from-sybil|mult
                  :sybils| sybils|
                  :send-krpc-request send-krpc-request
                  :count-torrentsA (atom 0)
                  :count-infohashes-from-samplingA (atom 0)
                  :count-infohashes-from-listeningA (atom 0)
                  :count-infohashes-from-sybilA (atom 0)
                  :count-discoveryA (atom 0)
                  :count-discovery-activeA (atom 0)
                  :count-messagesA count-messagesA
                  :count-messages-sybilA (atom 0)}

                 {:jframe nil
                  :jframe-title jframe-title
                  :resize| (chan (sliding-buffer 1))})

        state @stateA]

    (window)

    (go

      (println :self-id self-id)

      (Thorin.bittorrent-dht/process-routing-table
       (merge state {:routing-table-max-size 128}))


      (Thorin.bittorrent-dht/process-dht-keyspace
       (merge state {:routing-table-max-size 128}))

      (<! (onto-chan! nodes-to-sample|
                      (->> (:routing-table @stateA)
                           (vec)
                           (shuffle)
                           (take 8)
                           (map second))
                      false))

      (swap! stateA merge {:torrent| (let [out| (chan (sliding-buffer 100))
                                           torrent|tap (tap torrent|mult (chan (sliding-buffer 100)))]
                                       (go
                                         (loop []
                                           (when-let [value (<! torrent|tap)]
                                             (offer! out| value)
                                             (recur))))
                                       out|)})

      #_(go
          (<! (timeout duration))
          (stop))

      ; socket
      (Thorin.bittorrent-dht/process-socket state)

      ; save state to file periodically
      (go
        (when-not (Thorin.fs/path-exists? state-filepath)
          (<! (Thorin.seed/write-state-file state-filepath @stateA)))
        (loop []
          (<! (timeout (* 4.5 1000)))
          (<! (Thorin.seed/write-state-file state-filepath @stateA))
          (recur)))


      ; print info
      (let [stop| (chan 1)]
        (swap! procsA conj stop|)
        (process-print-info (merge state {:stop| stop|})))

      ; count
      (process-count state)


      ; after time passes, remove nodes from already-asked tables so they can be queried again
      (let [stop| (chan 1)]
        (swap! procsA conj stop|)
        (go
          (loop [timeout| (timeout 0)]
            (alt!
              timeout|
              ([_]
               (doseq [[id {:keys [timestamp]}] (:routing-table-sampled @stateA)]
                 (when (> (- (Thorin.seed/now) timestamp) (* 5 60 1000))
                   (swap! stateA update-in [:routing-table-sampled] dissoc id)))

               (doseq [[id {:keys [timestamp interval]}] (:routing-table-find-noded @stateA)]
                 (when (or
                        (and interval (> (Thorin.seed/now) (+ timestamp (* interval 1000))))
                        (> (- (Thorin.seed/now) timestamp) (* 5 60 1000)))
                   (swap! stateA update-in [:routing-table-find-noded] dissoc id)))
               (recur (timeout (* 10 1000))))

              stop|
              (do :stop)))))

      ; very rarely ask bootstrap servers for nodes
      (let [stop| (chan 1)]
        (swap! procsA conj stop|)
        (Thorin.bittorrent-find-nodes/process-bootstrap-query
         (merge state {:stop| stop|})))

      ; periodicaly ask nodes for new nodes
      (let [stop| (chan 1)]
        (swap! procsA conj stop|)
        (Thorin.bittorrent-find-nodes/process-dht-query
         (merge state {:stop| stop|})))

      ; start sybil
      #_(let [stop| (chan 1)]
          (swap! procsA conj stop|)
          (Thorin.bittorrent-sybil/process
           {:stateA stateA
            :nodes-bootstrap nodes-bootstrap
            :sybils| sybils|
            :infohash| infohashes-from-sybil|
            :stop| stop|
            :count-messages-sybilA count-messages-sybilA}))

      ; add new nodes to routing table
      (go
        (loop []
          (when-let [nodesBA (<! nodesBA|)]
            (let [nodes (Thorin.seed/decode-nodes nodesBA)]
              (>! routing-table-nodes| nodes)
              (>! dht-keyspace-nodes| nodes)
              (<! (onto-chan! nodes-to-sample| nodes false)))
            #_(println :nodes-count (count (:routing-table @stateA)))
            (recur))))

      ; ask peers directly, politely for infohashes
      (Thorin.bittorrent-sample-infohashes/process-sampling
       state)

      ; discovery
      (Thorin.bittorrent-metadata/process-discovery
       (merge state
              {:infohashes-from-sampling| (tap infohashes-from-sampling|mult (chan (sliding-buffer 100000)))
               :infohashes-from-listening| (tap infohashes-from-listening|mult (chan (sliding-buffer 100000)))
               :infohashes-from-sybil| (tap infohashes-from-sybil|mult (chan (sliding-buffer 100000)))}))

      ; process messages
      (Thorin.bittorrent-dht/process-messages
       state))))

(comment

  (require
   '[expanse.bytes.core :as bytes.core]
   '[Thorin.ipfs-dht :as Thorin.ipfs-dht]
   '[Thorin.db :as Thorin.db]
   :reload)

  (-main)

  (swap! stateA assoc :searchS "123")

  ;
  )

(defn process-print-info
  [{:as opts
    :keys [stateA
           data-dir
           stop|
           nodes-to-sample|
           nodes-from-sampling|
           sybils|
           count-infohashes-from-samplingA
           count-infohashes-from-listeningA
           count-infohashes-from-sybilA
           count-discoveryA
           count-discovery-activeA
           count-messagesA
           count-torrentsA
           count-messages-sybilA]}]
  (let [started-at (Thorin.seed/now)
        filepath (Thorin.fs/path-join data-dir "Thorin.bittorrent-dht-crawl.log.edn")
        _ (Thorin.fs/remove filepath)
        _ (Thorin.fs/make-parents filepath)
        writer (Thorin.fs/writer filepath :append true)
        countA (atom 0)
        release (fn []
                  (Thorin.protocols/close* writer))]
    (go
      (loop []
        (alt!
          (timeout (* 5 1000))
          ([_]
           (swap! countA inc)
           (let [state @stateA
                 info [[:count @countA]
                       [:infohashes [:total (+ @count-infohashes-from-samplingA @count-infohashes-from-listeningA @count-infohashes-from-sybilA)
                                     :sampling @count-infohashes-from-samplingA
                                     :listening @count-infohashes-from-listeningA
                                     :sybil @count-infohashes-from-sybilA]]
                       [:discovery [:total @count-discoveryA
                                    :active @count-discovery-activeA]]
                       [:torrents @count-torrentsA]
                       [:nodes-to-sample| (count (Thorin.seed/chan-buf nodes-to-sample|))
                        :nodes-from-sampling| (count (Thorin.seed/chan-buf nodes-from-sampling|))]
                       [:messages [:dht @count-messagesA :sybil @count-messages-sybilA]]
                       [:sockets @Thorin.bittorrent-metadata/count-socketsA]
                       [:routing-table (count (:routing-table state))]
                       [:dht-keyspace (map (fn [[id routing-table]] (count routing-table)) (:dht-keyspace state))]
                       [:routing-table-find-noded  (count (:routing-table-find-noded state))]
                       [:routing-table-sampled (count (:routing-table-sampled state))]
                       [:sybils| (str (- (Thorin.seed/fixed-buf-size sybils|) (count (Thorin.seed/chan-buf sybils|))) "/" (Thorin.seed/fixed-buf-size sybils|))]
                       [:time (str (int (/ (- (Thorin.seed/now) started-at) 1000 60)) "min")]]]
             (pprint info)
             (Thorin.protocols/write-string* writer (with-out-str (pprint info)))
             (Thorin.protocols/write-string* writer "\n"))
           (recur))

          stop|
          (do :stop)))
      (release))))

(defn process-count
  [{:as opts
    :keys [infohashes-from-sampling|mult
           infohashes-from-listening|mult
           infohashes-from-sybil|mult
           torrent|mult
           count-infohashes-from-samplingA
           count-infohashes-from-listeningA
           count-infohashes-from-sybilA
           count-torrentsA]}]
  (let [infohashes-from-sampling|tap (tap infohashes-from-sampling|mult (chan (sliding-buffer 100000)))
        infohashes-from-listening|tap (tap infohashes-from-listening|mult (chan (sliding-buffer 100000)))
        infohashes-from-sybil|tap (tap infohashes-from-sybil|mult (chan (sliding-buffer 100000)))
        torrent|tap (tap torrent|mult (chan (sliding-buffer 100)))]
    (go
      (loop []
        (let [[value port] (alts! [infohashes-from-sampling|tap
                                   infohashes-from-listening|tap
                                   infohashes-from-sybil|tap
                                   torrent|tap])]
          (when value
            (condp = port
              infohashes-from-sampling|tap
              (swap! count-infohashes-from-samplingA inc)

              infohashes-from-listening|tap
              (swap! count-infohashes-from-listeningA inc)

              infohashes-from-sybil|tap
              (swap! count-infohashes-from-sybilA inc)

              torrent|tap
              (swap! count-torrentsA inc))
            (recur)))))))





(comment

  (let [data-dir (io/file (System/getProperty "user.dir") "data")]
    (println data-dir)
    (println (.getCanonicalFile data-dir))
    (println (.getAbsolutePath data-dir)))

  ;;
  )



(comment

  clj -Sdeps '{:deps {org.clojure/clojure {:mvn/version "1.10.3"}
                      org.clojure/core.async {:mvn/version "1.3.618"}
                      expanse/bytes-jvm {:local/root "./expanse/bytes-jvm"}
                      expanse/codec-jvm {:local/root "./expanse/codec-jvm"}
                      expanse/core-jvm {:local/root "./expanse/core-jvm"}
                      expanse/datagram-socket-jvm {:local/root "./expanse/datagram-socket-jvm"}
                      expanse/socket-jvm {:local/root "./expanse/socket-jvm"}
                      expanse/fs-jvm {:local/root "./expanse/fs-jvm"}
                      expanse/fs-meta {:local/root "./expanse/fs-meta"}
                      expanse/transit-jvm {:local/root "./expanse/transit-jvm"}
                      expanse.bittorrent/spec {:local/root "./bittorrent/spec"}
                      expanse.bittorrent/bencode {:local/root "./bittorrent/bencode"}
                      expanse.bittorrent/wire-protocol {:local/root "./bittorrent/wire-protocol"}
                      expanse.bittorrent/dht-crawl {:local/root "./bittorrent/dht-crawl"}}} '(require '[expanse.bittorrent.dht-crawl.core :as dht-crawl.core] :reload-all)

  clj -Sdeps '{:deps {org.clojure/clojurescript {:mvn/version "1.10.844"}
                      org.clojure/core.async {:mvn/version "1.3.618"}
                      expanse/bytes-meta {:local/root "./expanse/bytes-meta"}
                      expanse/bytes-js {:local/root "./expanse/bytes-js"}
                      expanse/codec-js {:local/root "./expanse/codec-js"}
                      expanse/core-js {:local/root "./expanse/core-js"}
                      expanse/datagram-socket-nodejs {:local/root "./expanse/datagram-socket-nodejs"}
                      expanse/fs-nodejs {:local/root "./expanse/fs-nodejs"}
                      expanse/fs-meta {:local/root "./expanse/fs-meta"}
                      expanse/socket-nodejs {:local/root "./expanse/socket-nodejs"}
                      expanse/transit-js {:local/root "./expanse/transit-js"}

                      expanse.bittorrent/spec {:local/root "./bittorrent/spec"}
                      expanse.bittorrent/bencode {:local/root "./bittorrent/bencode"}
                      expanse.bittorrent/wire-protocol {:local/root "./bittorrent/wire-protocol"}
                      expanse.bittorrent/dht-crawl {:local/root "./bittorrent/dht-crawl"}}} '\
  -M -m cljs.main \
  -co '{:npm-deps {"randombytes" "2.1.0"
                   "bitfield" "4.0.0"
                   "fs-extra" "9.1.0"}
        :install-deps true
        :analyze-path "./bittorrent/dht-crawl"
        :repl-requires [[cljs.repl :refer-macros [source doc find-doc apropos dir pst]]
                        [cljs.pprint :refer [pprint] :refer-macros [pp]]]} '\
  -ro '{:host "0.0.0.0"
        :port 8899} '\
  --repl-env node --compile expanse.bittorrent.dht-crawl.core --repl

  (require
   '[clojure.core.async :as a :refer [chan go go-loop <! >!  take! put! offer! poll! alt! alts! close! onto-chan!
                                      pub sub unsub mult tap untap mix admix unmix pipe
                                      timeout to-chan  sliding-buffer dropping-buffer
                                      pipeline pipeline-async]]
   '[clojure.core.async.impl.protocols :refer [closed?]])

  (require
   '[Thorin.fs]
   '[Thorin.bytes]
   '[Thorin.codec]
   '[Thorin.bencode]
   '[expanse.bittorrent.dht-crawl.core :as dht-crawl.core]
   :reload #_:reload-all)

  (dht-crawl.core/start
   {:data-dir (Thorin.fs/path-join "./dht-crawl")})




  ;
  )



(comment

  (let [c| (chan 10 (map (fn [value]
                           (println [:mapping (.. (Thread/currentThread) (getName))])
                           (inc value))))]

    (go
      (loop [i 10]
        (when (> i 0)
          (<! (timeout 1000))
          (>! c| i)
          (recur (dec i))))
      (close! c|)
      (println [:exit 0]))

    (go
      (loop []
        (when-let [value (<! c|)]
          (println [:took value (.. (Thread/currentThread) (getName))])
          (recur)))
      (println [:exit 1]))

    (go
      (loop []
        (when-let [value (<! c|)]
          (println [:took value (.. (Thread/currentThread) (getName))])
          (recur)))
      (println [:exit 2]))

    (go
      (loop []
        (when-let [value (<! c|)]
          (println [:took value (.. (Thread/currentThread) (getName))])
          (recur)))
      (println [:exit 3]))

    (go
      (loop []
        (when-let [value (<! c|)]
          (println [:took value (.. (Thread/currentThread) (getName))])
          (recur)))
      (println [:exit 4])))

  ;
  )



(comment

  (let [stateA (atom (transient {}))]
    (dotimes [n 8]
      (go
        (loop [i 100]
          (when (> i 0)
            (get @stateA :n)
            (swap! stateA dissoc! :n)
            (swap! stateA assoc! :n i)


            (recur (dec i))))
        (println :done n))))

  ; java.lang.ArrayIndexOutOfBoundsException: Index -2 out of bounds for length 16
  ; at clojure.lang.PersistentArrayMap$TransientArrayMap.doWithout (PersistentArrayMap.java:432)
  ; at clojure.lang.ATransientMap.without (ATransientMap.java:69)
  ; at clojure.core$dissoc_BANG_.invokeStatic (core.clj:3373)


  (time
   (loop [i 10000000
          m (transient {})]
     (if (> i 0)

       (recur (dec i) (-> m
                          (assoc! :a 1)
                          (dissoc! :a)
                          (assoc! :a 2)))
       (persistent! m))))

  ; "Elapsed time: 799.025172 msecs"

  (time
   (loop [i 10000000
          m {}]
     (if (> i 0)

       (recur (dec i) (-> m
                          (assoc :a 1)
                          (dissoc :a)
                          (assoc :a 2)))
       m)))

  ; "Elapsed time: 1361.090409 msecs"

  (time
   (loop [i 10000000
          m (sorted-map)]
     (if (> i 0)

       (recur (dec i) (-> m
                          (assoc :a 1)
                          (dissoc :a)
                          (assoc :a 2)))
       m)))

  ; "Elapsed time: 1847.529152 msecs"

  ;
  )
