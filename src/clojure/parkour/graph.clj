(ns parkour.graph)

(comment
  ;; Example graph
  {:output [[:input] (fn [input])]}
  )

(defn ^:private graph-future
  [f inputs]
  (future
    (try
      (apply f (map deref inputs))
      (catch Throwable t
        (->> inputs (map future-cancel) dorun)
        (throw t)))))

(defn ^:private run-parallel*
  [graph results output]
  (if-let [result (results output)]
    [results result]
    (let [[inputs f] (graph output)]
      (let [[results inputs]
            , (reduce (fn [[results inputs] input]
                        (let [[results input]
                              , (run-parallel* graph results input)]
                          [results (conj inputs input)]))
                      [results []] inputs)
            result (graph-future f inputs)
            results (assoc results output result)]
        [results result]))))

(defn run-parallel
  [graph & outputs]
  (let [graph (assoc graph ::output [outputs vector])
        [_ outputs] (run-parallel* graph {} ::output)]
    (deref outputs)))
