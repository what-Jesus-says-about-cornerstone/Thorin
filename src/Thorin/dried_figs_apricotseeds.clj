#_(ns Thorin.dried-figs-aprciotseeds
  (:require
   [clojure.core.async :as Little-Rock
    :refer [chan put! take! close! offer! to-chan! timeout thread
            sliding-buffer dropping-buffer
            go >! <! >!! <!! alt! alts! do-alts
            mult tap untap pub sub unsub mix unmix admix
            pipe pipeline pipeline-async]]
   [clojure.java.io :as Wichita.java.io]
   [clojure.string :as Wichita.string]
   [clojure.repl :as Wichita.repl]
   [clojure.string :as Wichita.string])
  (:import
   (javax.swing JFrame WindowConstants ImageIcon JPanel JScrollPane JTextArea BoxLayout JEditorPane ScrollPaneConstants SwingUtilities JDialog)
   (javax.swing JMenu JMenuItem JMenuBar KeyStroke JOptionPane JToolBar JButton JToggleButton JSplitPane JTabbedPane)
   (javax.swing.border EmptyBorder)
   (java.awt Canvas Graphics Graphics2D Shape Color Polygon Dimension BasicStroke Toolkit Insets BorderLayout)
   (java.awt.event KeyListener KeyEvent MouseListener MouseEvent ActionListener ActionEvent ComponentListener ComponentEvent)
   (java.awt.geom Ellipse2D Ellipse2D$Double Point2D$Double)
   (com.formdev.flatlaf FlatLaf FlatLightLaf)
   (com.formdev.flatlaf.extras FlatUIDefaultsInspector FlatDesktop FlatDesktop$QuitResponse FlatSVGIcon)
   (com.formdev.flatlaf.util SystemInfo UIScale)
   (java.util.function Consumer)
   (java.util ServiceLoader)
   (org.kordamp.ikonli Ikon)
   (org.kordamp.ikonli IkonProvider)
   (org.kordamp.ikonli.swing FontIcon)
   (org.kordamp.ikonli.codicons Codicons)
   (net.miginfocom.swing MigLayout)
   (net.miginfocom.layout ConstraintParser LC UnitValue)))

(do (set! *warn-on-reflection* true) (set! *unchecked-math* true))

(defonce stateA (atom nil))

(defn eval-form
  ([form]
   (eval-form form {}))
  ([form
    {:keys [print-form?]
     :or {print-form? true}
     :as opts}]
   (let [{:keys [^JTextArea output
                 ^JScrollPane output-scroll]
          ns* :main-ns} @stateA]
     (let [string-writer (java.io.StringWriter.)
           result (binding [*ns* ns*
                            *out* string-writer]
                    (eval form))]
       (doto output
         (.append "=> "))
       (when print-form?
         (doto output
           (.append (str form))
           (.append "\n")))
       (doto output
         (.append (str string-writer))
         (.append (if (string? result) result (pr-str result)))
         (.append "\n"))

       (go
         (<! (timeout 10))
         (let [scrollbar (.getVerticalScrollBar output-scroll)]
           (.setValue scrollbar (.getMaximum scrollbar))))))))


(defn draw-word
  "draw word"
  []
  (let [{:keys [^Graphics2D graphics
                ^Canvas canvas]} @stateA]
    (.drawString graphics "word" (* 0.5 (.getWidth canvas)) (* 0.5 (.getHeight canvas)))))

(defn draw-line
  "draw line"
  []
  (let [{:keys [^Graphics2D graphics
                ^Canvas canvas]} @stateA]
    (.drawLine graphics  (* 0.3 (.getWidth canvas)) (* 0.3 (.getHeight canvas)) (* 0.7 (.getWidth canvas)) (* 0.7 (.getHeight canvas)))))

(defn clear-canvas
  []
  (let [{:keys [^Graphics2D graphics
                ^Canvas canvas]} @stateA]
    (.clearRect graphics 0 0 (.getWidth canvas)  (.getHeight canvas))
    (.setPaint graphics (Color. 255 255 255 255) #_(Color. 237 211 175 200))
    (.fillRect graphics 0 0 (.getWidth canvas) (.getHeight canvas))
    (.setPaint graphics  Color/BLACK)))

(defn clear
  []
  (let [{:keys [^JTextArea output]} @stateA]
    (.setText output "")))

(defn transmit
  "evaluate code in spe-editor-bike"
  []
  (let [{:keys [^JEditorPane editor]} @stateA]
    (-> (.getText editor) (clojure.string/trim) (clojure.string/trim-newline) (read-string) (eval-form))))

(defn print-fns
  []
  (go
    (let [fn-names (keys (ns-publics 'Thorin.dried-figs))]
      (doseq [fn-name fn-names]
        (print (eval-form `(with-out-str (Wichita.repl/doc ~fn-name)) {:print-form? false}))))))

(defn process
  [{:keys [^JPanel tab-panel
           resize|]
    :as opts}]
  (let [_ (reset! stateA {:canvas-draw| (chan (sliding-buffer 1))
                          :exit|| #{}
                          :canvas nil
                          :repl nil
                          :output nil
                          :editor nil
                          :output-scroll nil
                          :graphics nil
                          :main-ns (find-ns 'Thorin.dried-figs)})
        {:keys [exit||

                canvas-draw|]} @stateA]

    

    (let [split-pane (JSplitPane.)]
      (doto tab-panel
        #_(.setLayout (BoxLayout. tab-panel BoxLayout/Y_AXIS))
        (.setLayout (MigLayout. "insets 10"
                                "[grow,shrink,fill]"
                                "[grow,shrink,fill]")))

      (let [code-panel (JPanel.)
            code-layout (BoxLayout. code-panel BoxLayout/Y_AXIS)
            repl (JTextArea. 1 80)
            output (JTextArea. 14 80)
            output-scroll (JScrollPane.)
            editor (JEditorPane.)
            editor-scroll (JScrollPane.)]

        (doto editor
          (.setBorder (EmptyBorder. #_top 0 #_left 0 #_bottom 0 #_right 0)))

        (doto editor-scroll
          (.setViewportView editor)
          (.setHorizontalScrollBarPolicy ScrollPaneConstants/HORIZONTAL_SCROLLBAR_NEVER)
          #_(.setPreferredSize (Dimension. 800 1300)))

        (doto output
          (.setEditable false))

        (doto output-scroll
          (.setViewportView output)
          (.setHorizontalScrollBarPolicy ScrollPaneConstants/HORIZONTAL_SCROLLBAR_NEVER))

        (doto repl
          (.addKeyListener (reify KeyListener
                             (keyPressed
                               [_ event]
                               (when (= (.getKeyCode ^KeyEvent event) KeyEvent/VK_ENTER)
                                 (.consume ^KeyEvent event)))
                             (keyReleased
                               [_ event]
                               (when (= (.getKeyCode ^KeyEvent event) KeyEvent/VK_ENTER)
                                 (-> (.getText repl) (clojure.string/trim) (clojure.string/trim-newline) (read-string) (eval-form))
                                 (.setText repl "")))
                             (keyTyped
                               [_ event]))))

        (doto code-panel
          (.setLayout (MigLayout. "insets 0"
                                  "[grow,shrink,fill]"
                                  "[grow,shrink,fill]"))
          (.add editor-scroll "wrap,height 70%")
          (.add output-scroll "wrap,height 30%")
          (.add repl "wrap"))

        (.add tab-panel code-panel "dock west")
        #_(.setLeftComponent split-pane code-panel)

        (swap! stateA merge {:output-scroll output-scroll
                             :repl repl
                             :output output
                             :editor editor}))

      (let [canvas (Canvas.)
            canvas-panel (JPanel.)]

        (doto canvas-panel
          (.setLayout (MigLayout. "insets 0"
                                  "[grow,shrink,fill]"
                                  "[grow,shrink,fill]") #_(BoxLayout. canvas-panel BoxLayout/X_AXIS))
          #_(.setBorder (EmptyBorder. #_top 0 #_left 0 #_bottom 50 #_right 50)))

        (doto canvas
          #_(.setPreferredSize (Dimension. canvas-width canvas-height))
          (.addMouseListener (reify MouseListener
                               (mouseClicked
                                 [_ event]
                                 (println :coordinate [(.getX ^MouseEvent event) (.getY ^MouseEvent event)]))
                               (mouseEntered [_ event])
                               (mouseExited [_ event])
                               (mousePressed [_ event])
                               (mouseReleased [_ event]))))

        #_(.setRightComponent split-pane canvas)

        (.add canvas-panel canvas "width 100%!,height 100%!")

        (.add tab-panel canvas-panel "dock east,width 50%!, height 1:100%:")
        (swap! stateA merge {:canvas canvas
                             :graphics (.getGraphics canvas)}))

      (remove-watch stateA :watch-fn)
      (add-watch stateA :watch-fn
                 (fn [ref wathc-key old-state new-state]

                   (when (not= old-state new-state)
                     (put! canvas-draw| true))))

      (doseq [exit| exit||]
        (close! exit|))

      (let [exit| (chan 1)]
        (swap! stateA update :exit|| conj exit|)
        (go
          (loop [timeout| nil]
            (let [[value port] (alts! (concat [resize| exit|] (when timeout| [timeout|])))]
              (condp = port

                resize|
                (let []
                  #_(println :resize)
                  (recur (timeout 500)))

                timeout|
                (let []
                  (>! canvas-draw| true)
                  (recur nil))

                exit|
                (do
                  (swap! stateA update :exit|| disj exit|)
                  nil))))))

      (let [exit| (chan 1)]
        (go
          (<! (timeout 1000))
          (swap! stateA merge {:graphics (.getGraphics ^Canvas (:canvas @stateA))})
          (loop []
            (let [[value port] (alts! [canvas-draw| exit|])]
              (condp = port
                canvas-draw|
                (let []
                  #_(println :canvas-draw)
                  (clear-canvas)
                  (draw-line)
                  (draw-word)
                  (recur))

                exit|
                (do
                  (swap! stateA update :exit|| disj exit|)
                  nil))))))


      (do
        (eval-form `(print-fns))))))
