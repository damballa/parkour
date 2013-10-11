(ns parkour.graph.common
  {:private true})

(defn ^:private job-graph*
  [uvar rargs largs]
  (let [graph (uvar rargs largs)
        tails (if (vector? graph) graph [graph])]
    (->> (iterate (partial mapcat :requires) tails)
         (take-while seq)
         (apply concat)
         (reduce (fn [[_ jids jobs :as state] {:keys [sink], :as node}]
                   (if-let [jid (jids sink)]
                     state
                     (let [jid (count jids)
                           node (assoc node :jid jid, :uvar uvar, :rargs rargs)]
                       [tails (assoc jids sink jid) (conj jobs node)])))
                 [tails {} []]))))

(defn job-graph
  [uvar rargs largs]
  (let [[tails jids jobs] (job-graph* uvar rargs largs)
        rjid (partial - (-> jids count dec))
        rjids (comp rjid jids)]
    [(mapv (fn [{:keys [jid requires], :or {requires []}, :as node}]
             (let [jid (rjid jid), requires (mapv (comp rjids :sink) requires)]
               (assoc node :jid jid :requires requires)))
           (rseq jobs))
     (mapv (comp rjids :sink) tails)]))
