(ns parkour.graph.common
  {:private true})

(defn ^:private job-graph*
  [uvar rargs largs]
  (let [graph (uvar rargs largs)
        tails (if (vector? graph) graph [graph])]
    (->> (iterate (partial mapcat :requires) tails)
         (take-while seq)
         (apply concat)
         (reduce (fn [[_ jids jobs :as state] {:keys [sink-id], :as node}]
                   (if-let [jid (jids sink-id)]
                     state
                     (let [jid (count jids)
                           node (assoc node :jid jid, :uvar uvar, :rargs rargs)]
                       [tails (assoc jids sink-id jid) (conj jobs node)])))
                 [tails {} []]))))

(defn job-graph
  [uvar rargs largs]
  (let [[tails jids jobs] (job-graph* uvar rargs largs)
        jid->rjid (partial - (-> jids count dec))
        rjids (comp jid->rjid jids), rjid (comp rjids :sink-id)]
    [(mapv (fn [{:keys [jid requires], :or {requires []}, :as node}]
             (let [jid (jid->rjid jid), requires (mapv rjid requires)]
               (assoc node :jid jid :requires requires)))
           (rseq jobs))
     (mapv rjid tails)]))
