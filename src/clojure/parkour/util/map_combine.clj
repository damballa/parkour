(ns parkour.util.map-combine
  (:import [java.util.concurrent
            , Executors ExecutorService Future TimeUnit ThreadPoolExecutor]))

(def ^:private ^ThreadPoolExecutor pool
  "Default thread pool for map-combine task execution."
  (let [n (-> (Runtime/getRuntime) .availableProcessors (+ 2))
        ^ThreadPoolExecutor pool (Executors/newFixedThreadPool n)]
    (doto pool
      (.setKeepAliveTime 30 TimeUnit/SECONDS)
      (.allowCoreThreadTimeOut true))))

(defn ^:private mc-task
  [state result combinef mapf x]
  (fn []
    (try
      (let [x (mapf x), f (fn [[n r]] [(dec n) (combinef r x)])
            [n r] (swap! state f)]
        (when (zero? n)
          (deliver result [:success r])))
      (catch Exception e
        (deliver result [:failure e])))))

(defn map-combine
  "Apply `mapf` to each element of `coll`, then combine those results with
`combinef`.  The `combinef` function must return its identity element when
called with no arguments.  Each step may occur in parallel and the map results
may be combined in any order."
  ([mapf combinef coll] (map-combine pool mapf combinef coll))
  ([^ExecutorService pool mapf combinef coll]
     (let [state (atom [(count coll) (combinef)]), result (promise)
           futf (partial mc-task state result combinef mapf)
           futs (mapv #(.submit pool ^Callable (futf %)) coll)
           [status result] (try @result (catch Exception e [:failure e]))]
       (if (identical? :success status)
         result
         (do
           (doseq [^Future fut futs] (.cancel fut true))
           (throw result))))))
