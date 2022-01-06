(ns metabase.query-processor.middleware.add-references
  (:require [clojure.walk :as walk]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.middleware.add-implicit-clauses :as add-implicit-clauses]
            [metabase.query-processor.store :as qp.store]
            [metabase.util :as u]
            [metabase.query-processor.middleware.annotate :as annotate]))

(defn- source-table-references [source-table-id join-alias]
  (when source-table-id
    (let [field-clauses (add-implicit-clauses/sorted-implicit-fields-for-table source-table-id)
          field-ids     (into #{} (mbql.u/match field-clauses
                                    [:field (id :guard int?) _]
                                    id))]
      ;; make sure the Fields are in the store. They might not be if this is a Table from a join where we don't select
      ;; all its fields
      (qp.store/fetch-and-store-fields! field-ids)
      (into
       {}
       (map (fn [clause]
              [clause (if join-alias
                        {:source {:table join-alias}}
                        {})]))
       field-clauses))))

(defn- add-alias-info [refs]
  (into
   {}
   (map (fn [[clause info]]
          (mbql.u/match-one clause
            [:field id-or-name _opts]
            (let [field      (when (integer? id-or-name)
                               (qp.store/field id-or-name))
                  field-name (if (string? id-or-name)
                               id-or-name
                               (:name field))
                  table      (some-> (:table_id field) qp.store/table)]
              [clause (assoc info :alias field-name)])

            _
            [clause info])))
   refs))

(defn- selected-references [{:keys [fields breakout aggregation]}]
  (into
   {}
   (comp cat
         (map-indexed
          (fn [i clause]
            (mbql.u/match-one clause
              [:aggregation-options _ opts]
              [[:aggregation (::index opts)] {:position i, :alias (:name opts)}]

              _
              [clause {:position i}]))))
   [breakout
    (map-indexed
     (fn [i ag]
       (mbql.u/replace ag
         [:aggregation-options wrapped opts]
         [:aggregation-options wrapped (assoc opts ::index i)]))
     aggregation)
    fields]))

;; TODO FIXME need to make sure we handle native source queries -- determine refs from `:source-metadata` (especially for native queries!)
(defn- source-query-references
  [{refs :qp/refs, aggregations :aggregation, :as source-query} _source-metadata]
  (into
   {}
   (comp (filter (fn [[_clause info]]
                   (:position info)))
         (map (fn [[clause info]]
                (let [clause (mbql.u/match-one clause
                               :field
                               &match

                               [:aggregation index]
                               (let [ag-clause              (get aggregations index)
                                     {base-type :base_type} (annotate/col-info-for-aggregation-clause source-query ag-clause)]
                                 [:field (:alias info) {:base-type base-type}]))
                      info (-> info
                               (assoc :source {:table "source", :alias (:alias info)})
                               (dissoc :position))]
                  [clause info]))))
   refs))

(defn- join-references [joins]
  (into
   {}
   (mapcat (fn [{join-alias :alias, refs :qp/refs}]
             (for [[clause info] refs
                   :let          [info (assoc info
                                              :source {:table join-alias, :alias (:alias info)}
                                              ;; TODO -- this needs to use [[metabase.driver.sql.query-processor/prefix-field-alias]]
                                              ;; or some new equivalent method
                                              :alias (format "%s__%s" join-alias (or (get-in info [:source :alias])
                                                                                     (:alias info))))]]
               (mbql.u/match-one clause
                 [:field id-or-name opts] [[:field id-or-name (assoc opts :join-alias join-alias)] info]
                 _                        [clause info]))))
   joins))

(defn- uniquify-visible-ref-aliases [refs]
  (let [unique-name (mbql.u/unique-name-generator)]
    (into
     {}
     (map (fn [[clause info]]
            [clause (cond-> info
                      (:position info) (update :alias unique-name))]))
     refs)))

(defn- recursive-merge [& maps]
  (if (every? (some-fn nil? map?) maps)
    (apply merge-with recursive-merge maps)
    (last maps)))

(defn- add-references*
  [{:keys [source-table source-query source-metadata joins]
    join-alias :alias
    refs :qp/refs
    :as inner-query}]
  (let [refs (-> (recursive-merge
                  (add-alias-info (source-table-references source-table join-alias))
                  (add-alias-info (selected-references inner-query))
                  (source-query-references source-query source-metadata)
                  (join-references joins))
                 uniquify-visible-ref-aliases)]
    (assoc inner-query :qp/refs refs)))

(defn- remove-join-references [x]
  (mbql.u/replace x
    (m :guard (every-pred map? :alias :qp/refs))
    (remove-join-references (dissoc m :qp/refs))))

(defn add-references [query]
  (let [query (walk/postwalk
               (fn [form]
                 (if (and (map? form)
                          ((some-fn :source-query :source-table) form))
                   (add-references* form)
                   form))
               query)]
    (remove-join-references query)))
