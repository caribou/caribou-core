(ns caribou.test.model
  (:use [caribou.debug]
        [caribou.model]
        [clojure.test])
  (:require [clojure.java.jdbc :as sql]
            [clojure.java.io :as io]
            [caribou.db :as db]
            [caribou.auth :as auth]
            [caribou.util :as util]
            [caribou.validation :as validation]
            [caribou.config :as config]))

(defn test-init
  []
  (invoke-models)
  (clear-queries))

(defn db-fixture
  [f]
  (sql/with-connection @config/db
    (test-init)
    (f)
    (doseq [slug [:yellow :purple :zap :chartreuse :fuchsia :base :level
                  :void :white :agent]]
      (when (db/table? slug) (destroy :model (-> @models slug :id)))
      (when (db/table? :everywhere)
        (do
          (db/do-sql "delete from locale")
          (destroy :model (-> @models :nowhere :id))
          (destroy :model (-> @models :everywhere :id))))
      (when-let [passport (pick :asset {:where
                                        {:filename "passport.picture"}})]
        (destroy :asset (:id passport))))))

(defn invoke-model-test
  []
  (let [model (util/query "select * from model where id = 1")
        invoked (invoke-model (first model))]
    (testing "Model invocation."
      (is (= "name" (-> invoked :fields :name :row :slug))))))

(defn model-lifecycle-test
  []
  (let [model (create :model
                      {:name "Yellow"
                       :description "yellowness yellow yellow"
                       :position 3
                       :fields [{:name "Gogon" :type "string"}
                                {:name "Wibib" :type "boolean"}]})
        yellow (create :yellow {:gogon "obobo" :wibib true})]
    (testing "Model life cycle verification."
      (is (<= 8 (count (-> @models :yellow :fields))))
      (is (= (model :name) "Yellow"))
      (is ((models :yellow) :name "Yellow"))
      (is (db/table? :yellow))
      (is (yellow :wibib))
      (is (= 1 (count (util/query "select * from yellow"))))

      (destroy :model (model :id))

      (is (not (db/table? :yellow)))
      (is (not (models :yellow))))))

(defn model-interaction-test
  []
  (let [yellow-row (create :model
                           {:name "Yellow"
                            :description "yellowness yellow yellow"
                            :position 3
                            :fields [{:name "Gogon" :type "string"}
                                     {:name "Wibib" :type "boolean"}]})

        zap-row (create :model
                        {:name "Zap"
                         :description "zap zappity zapzap"
                         :position 3
                         :fields [{:name "Ibibib" :type "string"}
                                  {:name "Yobob" :type "slug"
                                   :link_slug "ibibib"}
                                  {:name "Yellows" :type "collection"
                                   :dependent true
                                   :target_id (yellow-row :id)}]})

        yellow (models :yellow)
        zap (models :zap)

        zzzap (create :zap {:ibibib "kkkkkkk"})
        yyy (create :yellow {:gogon "obobo" :wibib true :zap_id (zzzap :id)})
        yyyz (create :yellow {:gogon "igigi" :wibib false :zap_id (zzzap :id)})
        yy (create :yellow {:gogon "lalal" :wibib true :zap_id (zzzap :id)})]
    (update :yellow (yyy :id) {:gogon "binbin"})
    (update :zap (zzzap :id)
            {:ibibib "OOOOOO mmmmm   ZZZZZZZZZZ"
             :yellows [{:id (yyyz :id) :gogon "IIbbiiIIIbbibib"}
                       {:gogon "nonononononon"}]})

    (testing "Model interactions."
      (let [zap-reload (db/choose :zap (zzzap :id))]
        (is (= ((db/choose :yellow (yyyz :id)) :gogon) "IIbbiiIIIbbibib"))
        (is (= ((db/choose :yellow (yyy :id)) :gogon) "binbin"))
        (is (= (zap-reload :yobob) "oooooo_mmmmm_zzzzzzzzzz"))
        (is (= "OOOOOO mmmmm   ZZZZZZZZZZ" ((from zap zap-reload {:include {}})
                                            :ibibib)))
        (is (= 4 (count ((from zap zap-reload {:include {:yellows {}}})
                         :yellows))))

        (update :model (zap :id)
                {:fields [{:id (-> zap :fields :ibibib :row :id)
                           :name "Okokok"}]})

        (update :model (yellow :id) {:name "Purple"
                                     :fields [{:id
                                               (-> yellow :fields :zap :row :id)
                                               :name "Green"}]})

        (let [zappo (db/choose :zap (zzzap :id))
              purple (db/choose :purple (yyy :id))]
          (is (= (zappo :okokok) "OOOOOO mmmmm   ZZZZZZZZZZ"))
          (is (= (purple :green_id) (zappo :id))))

        (destroy :zap (:id zap-reload))
        (let [purples (util/query "select * from purple")]
          (is (empty? purples))))

      (destroy :model (zap :id))

      (is (empty? (-> @models :purple :fields :green_id)))

      (destroy :model (-> @models :purple :id))

      (is (and (not (db/table? :purple))
               (not (db/table? :yellow))
               (not (db/table? :zap)))))))

(defn model-link-test
  []
  (let [chartreuse-row
        (create :model
                {:name "Chartreuse"
                 :description "chartreusey reuse chartreuse"
                 :position 3
                 :fields [{:name "Ondondon" :type "string"}
                          {:name "Kokok" :type "boolean"}]})

        fuchsia-row
        (create :model
                {:name "Fuchsia"
                 :description "fuchfuchsia siasiasia fuchsia"
                 :position 3
                 :fields [{:name "Zozoz" :type "string"}
                          {:name "Chartreusii" :type "link" :dependent true
                           :target_id (chartreuse-row :id)}]})

        chartreuse (models :chartreuse)
        fuchsia (models :fuchsia)
        charfuch (models :chartreusii_fuchsia)

        cf-link (-> chartreuse :fields :fuchsia)
        fc-link (-> fuchsia :fields :chartreusii)

        ccc (create :chartreuse {:ondondon "obobob" :kokok true})
        cdc (create :chartreuse {:ondondon "ikkik" :kokok false})
        cbc (create :chartreuse {:ondondon "zozoozozoz" :kokok false})

        fff (create :fuchsia {:zozoz "glowing"})
        fgf (create :fuchsia {:zozoz "torpid"})
        fef (create :fuchsia {:zozoz "bluish"})]
    ;; make some links
    (link cf-link ccc fff)
    (link cf-link cdc fff)
    (link fc-link fgf cbc)

    ;; create links through update rather than directly
    (update :fuchsia (fef :id) {:chartreusii [cbc ccc {:ondondon "ikikik"
                                                       :kokok false
                                                       :fuchsia
                                                       [{:zozoz "granular"}]}]})

    (testing "Model links."
      (let [fff-X (from (models :fuchsia) fff {:include {:chartreusii {}}})
            cec (pick :chartreuse {:where {:ondondon "ikikik"}
                                   :include {:fuchsia {}}})]
        (is (= 2 (count (retrieve-links cf-link ccc))))
        (is (= 2 (count (fff-X :chartreusii))))
        (is (some #(= % "granular") (map :zozoz (:fuchsia cec))))

        (update :model (:id chartreuse)
                {:fields [{:id (-> cf-link :row :id) :name "Nightpurple"
                           :slug "nightpurple"}]})

        (let [chartreuse (models :chartreuse)
              coc (gather
                   :chartreuse
                   {:where {:nightpurple {:zozoz "granular"}}
                    :order {:nightpurple {:id :desc}}
                    :include {:nightpurple {}}
                    :limit 5
                    :offset 0})]
          (is (= 1 (count coc)))
          (is (= 2 (count (:nightpurple (first coc)))))
          (is (present? (models :chartreusii_nightpurple)))
          (let [falses (gather :chartreuse {:where {:kokok false}})]
            (is (= 3 (count falses)))))))))

(defn parallel-include-test
  []
  (let [base-row (create
                  :model
                  {:name "Base"
                   :fields [{:name "Thing" :type "string"}]})
        level-row (create
                   :model
                   {:name "Level"
                    :fields [{:name "Strata" :type "integer"}
                             {:name "Base" :type "part"
                              :target_id (:id base-row)
                              :dependent true :reciprocal_name "Levels"}]})
        void-row (create
                  :model
                  {:name "Void"
                   :fields [{:name "Tether" :type "string"}
                            {:name "Base" :type "part" :target_id (:id base-row)
                             :dependent true :reciprocal_name "Void"}]})

        base-a (create :base {:thing "AAAA" :position 1
                              :levels [{:strata 5} {:strata 8} {:strata -3}]
                              :void [{:tether "ooo"} {:tether "vvv"}
                                     {:tether "xxx"}]})
        base-b (create :base {:thing "BBBB" :position 2
                              :levels [{:strata 12} {:strata 88} {:strata 33}]
                              :void [{:tether "xxx"} {:tether "lll"}]})
        base-c (create :base {:thing "CCCC" :position 3
                              :levels [{:strata 5} {:strata -8} {:strata -3}]
                              :void [{:tether "mmm"} {:tether "ddd"}]})
        base-d (create :base {:thing "DDDD" :position 4
                              :levels [{:strata 5} {:strata -11} {:strata -33}]
                              :void [{:tether "xxx"}]})
        base-e (create :base {:thing "EEEE" :position 5
                              :levels [{:strata 5} {:strata -8} {:strata -3}]
                              :void [{:tether "lll"} {:tether "xxx"}
                                     {:tether "www"}]})

        voids (gather :void)

        bases (gather
               :base
               {:include {:levels {} :void {}}
                :where {:levels {:strata 5} :void {:tether "xxx"}}
                :limit 1 :offset 1
                :order {:position :asc}})]

    (testing "Parallel model includes."
      (is (= 11 (count voids)))
      (is (= 1 (count bases)))
      (is (= "DDDD" (-> bases first :thing))))
    ;; these could be run without the db if we could create all the fields
    ;; in models without the db
    (testing "Proper usage of string coercion in beam-validator."
      ;; this is good if no exception is thrown
      (is (not (validation/beams "base" {:include {:levels {}}}))))
    (testing "Detection of invalid models in beam-validator."
      (is (thrown-with-msg? Exception #"no such model"
            ;; with gather this error is caught higher up currently
            ;; (gather :cheese {:include {:rennet {}}}))))
            (validation/beams :cheese {:include {:rennet {}}}))))
    (testing "Validation of includes and fields with nesting in beam-validator."
      (is (thrown-with-msg? Exception
            #"field of type .* cannot be included"
            ;; for now beam-validator is just having it's exception caught and
            ;; printed, until it is ready for prime time
            ;; (gather :base {:include {:thing {}}})))
            (validation/beams :base {:include {:thing {}}})))
      (is (thrown-with-msg? Exception
            #"field .* not found in model"
            ;; for now beam-validator is just having it's exception caught and
            ;; printed, until it is ready for prime time
            ;; (gather :base {:include {:levels {:invalid {}}}})))
            (validation/beams :base {:include {:levels {:invalid {}}}})))
      (is (thrown-with-msg? Exception
            #"field .* not found in model"
            ;; for now beam-validator is just having it's exception caught and
            ;; printed, until it is ready for prime time
            ;; (gather :base {:include {:levels {} :void {}}
            ;;                :where {:levels {:strata 5
            ;;                :invalid_field nil}}}))))))
            (validation/beams :base
                              {:include {:levels {} :void {}}
                               :where {:levels {:strata 5
                                                :invalid_field nil}}}))))))

(defn localized-model-test
  []
  (let [place (create :locale {:language "Ibeo" :region "Glass" :code "ib_or"})
        everywhere (create
                    :model
                    {:name "Everywhere" :localized true
                     :fields [{:name "Up" :type "string"}
                              {:name "Grass" :type "text"}
                              {:name "Through" :type "boolean"}
                              {:name "Form Structure" :type "asset"}
                              {:name "Colocate" :type "address"}
                              ;; {:name "Whentime" :type "timestamp"}
                              {:name "Under" :type "decimal"}]})
        other (create :locale {:language "Gornon" :region "Ipipip"
                               :code "go_xb"})
        nowhere (create
                 :model
                 {:name "Nowhere" :localized true
                  :fields [{:name "Down" :type "string"}
                           {:name "Everywhere" :type "link" :dependent true
                            :target_id (everywhere :id)}]})
        a (create :everywhere {:up "Hey" :grass "On" :through true :under 10.1})
        b (create :everywhere {:up "What" :grass "Bead" :through true
                               :under 33.333})
        c (create :everywhere {:up "Is" :grass "Growth" :through false
                               :under 22222})
        xxx (create :nowhere {:down "Ylel" :everywhere [{:id (:id a)}]})
        outer (create :locale {:language "Obooe" :region "Xorxox"
                               :code "xo_ub"})
        other-other (update :locale (:id other) {:code "bx_pa"})

        _ (update :model (:id everywhere)
                  {:fields [{:id (-> @models :everywhere :fields :grass :row
                                     :id)
                             :name "Blade" :slug "blade"}]})

        xxx-other (update :nowhere (:id xxx) {:down "IiiiiiIIIIIII"}
                          {:locale "xo_ub"})
        xxx-other (update :nowhere (:id xxx) {:down "Prortrobr"
                                              :everywhere [{:id (:id b)}
                                                           {:id (:id c)}]}
                          {:locale "bx_pa"})
        xxx-other (update :nowhere (:id xxx) {:down "Grungruublor"
                                              :everywhere [{:id (:id b)}]}
                          {:locale "ib_or"})

        xo-ub-eees
        (gather
         :everywhere
         {:include {:nowhere {}}
          :where {:nowhere {:down "IiiiiiIIIIIII"}}
          :order {:nowhere {:down :desc}}
          :limit 3
          :locale "xo_ub"})

        bx-pa-eees
        (gather
         :everywhere
         {:include {:nowhere {}}
          :where {:nowhere {:down "Prortrobr"}}
          :order {:nowhere {:down :desc}}
          :limit 3
          :locale "bx_pa"})

        ib-or-eees
        (gather
         :everywhere
         {:include {:nowhere {}}
          :where {:nowhere {:down "Grungruublor"}}
          :order {:nowhere {:down :desc}}
          :limit 1
          :locale "ib_or"})

        ordered-everywhere
        (gather
         :everywhere
         {:order {:blade :desc}
          :limit 2
          :offset 1})

        joins
        (gather
         :everywhere_nowhere
         {:include {:everywhere {}}
          :where {:everywhere {:through true}}
          :order {:everywhere {:under :asc}}})

        bx-joins
        (gather
         :everywhere_nowhere
         {:include {:everywhere {}}
          :where {:everywhere {:through true}}
          :order {:everywhere {:under :asc}}
          :locale "bx_pa"})

        nowhat
        (pick
         :nowhere
         {:where {:id (:id xxx)}
          :locale "bx_pa"})

        everywhat
        (pick
         :everywhere
         {:where {:id (:id c)}
          :locale "xo_ub"})]

    (testing "Locales."
      (is (= 1 (count xo-ub-eees)))
      (is (= 3 (count bx-pa-eees)))
      (is (= 1 (count ib-or-eees)))
      (is (= '("Growth" "Bead") (map :blade ordered-everywhere)))
      (is (= 1 (count joins)))
      (is (= "Hey" (-> joins first :everywhere :up)))
      (is (= "Prortrobr" (:down nowhat)))
      (is (= "Is" (:up everywhat)))

      (is (= 2 (count bx-joins)))
      (is (= '("Hey" "What") (map #(-> % :everywhere :up) bx-joins)))

      (println (form-uberquery
                (@models :everywhere)
                {:include {:nowhere {}}
                 :where {:nowhere {:down "Ylel"}}
                 :order {:nowhere {:down :desc}}
                 :limit 3
                 :locale "xo_ub"})))))

(defn nested-model-test
  []
  ;; this is also implicitly verifying :tie fields
  (let [white (create :model {:name "White" :nested true
                              :fields [{:name "Grey" :type "string"}]})
        aaa (create :white {:grey "obobob"})
        bbb (create :white {:grey "ininin" :parent_id (aaa :id)})
        ccc (create :white {:grey "kkukku" :parent_id (aaa :id)})
        ddd (create :white {:grey "zezeze" :parent_id (bbb :id)})
        eee (create :white {:grey "omomom" :parent_id (ddd :id)})
        fff (create :white {:grey "mnomno" :parent_id (ddd :id)})
        ggg (create :white {:grey "jjijji" :parent_id (ccc :id)})
        tree (arrange-tree [aaa bbb ccc ddd eee fff ggg])]
    ;; fff_path (progenitors :white (fff :id))
    ;; bbb_children (descendents :white (bbb :id))]
    ;; (is (= 4 (count fff_path)))
    ;; (is (= 4 (count bbb_children))))
    (testing "Nested models."
      (doseq [branch tree]
        (println (doall (str tree))))
      (is (= 1 (count tree))))))

;; (defn migration-test
;;   []
;;   (sql/with-connection @config/db
;;     (init)))

(defn fields-types-test
  []
  (let [fields (map (fn [[n t]] {:name n :type t})
                    [["nchildren" "integer"]
                     ["height" "decimal"]
                     ["name" "string"]
                     ["auth" "password"]
                     ["round" "slug"]
                     ["bio" "text"]
                     ["adequate" "boolean"]
                     ["born_at" "timestamp"]
                     ["dies_at" "timestamp"]
                     ["passport" "asset"]
                     ["residence" "address"]])
        agent (create :model {:name "Agent"
                              :fields fields})
        passport (create :asset {:filename "passport.picture"})
        bond-height 69.55
        bond-values {:nchildren 32767
                     :height bond-height
                     :name (str "James Bond" (util/rand-str 245))
                     :auth "Octopu55y"
                     :round ".38 Hollow Tip"
                     ;; mysql text type size issue
                     :bio (str "He did stuff: ☃" (util/rand-str 65500))
                     :adequate true
                     ;; mysql date-time precision issue
                     :born_at "January 1 1970"
                     ;; mysql date-time precision issue
                     :dies_at "January 1 2037"
                     :passport_id (:id passport)
                     ;; connectivity issues prevent this from working
                     ;; :residence {:address "WHITE HOUSE"
                     ;;             :country "USA"}
                     }
        bond (create :agent bond-values)
        agent-id (:id agent)
        bond-id (:id bond)
        bond (pick :agent {:where {:id bond-id}})
        ;; amount of floating point error that is allowable
        ;; mysql precision issue
        EPSILON 0.000001
        run-field-tests
        (fn []
          (testing "Valid properties of field types."
            (is (seq agent))
            (is (seq bond))
            ;; id fields are implicit
            (is (number? (:id agent)))
            (is (number? (:id bond)))
            (is (= (:nchildren bond) 32767))
            (is (< (java.lang.Math/abs (- bond-height (:height bond))) EPSILON))
            ;; case sensitivity
            (is (= "James Bond" (subs (:name bond) 0 10)))
            (is (= (count (:name bond)) 255))
            (is (not (= (:auth bond) "Octopu55y")))
            (is (auth/check-password "Octopu55y" (:auth bond)))
            ;; (is (= "_8_hollow_tip" (:round bond)))
            ;; unicode
            (is (= "He did stuff: ☃" (subs (:bio bond) 0 15)))
            (is (= (count (:bio bond)) 65515))
            ;; if this breaks...
            (is (:adequate bond))
            ;; unix time of 1/1/1970 / 1/1/2037
            (is (and (= 28800000 (.getTime (:born_at bond)))
                     (= 2114409600000 (.getTime (:dies_at bond)))))
            (is (= "passport.picture" (-> bond :passport :filename)))
            ;; (is (and (= 0.0 (-> bond :residence :lat))
            ;;          (= 0.0 (-> bond :residence :long))))
            ))]
    (run-field-tests)
    (testing "Deletion of fields."
      (is (nil?
           (let [field-slugs (map (comp keyword :name) fields)
                 field-ids (map (fn [slug] (-> agent :fields slug :row :id))
                                field-slugs)
                 field-ids (filter number? field-ids)]
             (println "field slugs" field-slugs)
             (println "field ids" field-ids)
             (doseq [id field-ids]
               (destroy :field id))))))
    (testing "Addition of fields to a model."
      (is (nil? (doseq [field-spec fields]
                 (create :field (merge field-spec {:model_id agent-id}))))))
    (testing "Updating field values."
      (is (nil? (doseq [update-spec bond-values]
                  (update :agent bond-id (into {} [update-spec]))))))
    (run-field-tests)))


(deftest ^:mysql
  mysql-tests
  (let [config (config/read-config (io/resource "config/test-mysql.clj"))]
    (config/configure config)
    (db-fixture invoke-model-test)
    (db-fixture model-lifecycle-test)
    (db-fixture model-interaction-test)
    (db-fixture model-link-test)
    (db-fixture parallel-include-test)
    (db-fixture localized-model-test)
    (db-fixture nested-model-test)
    (db-fixture fields-types-test)))

(deftest ^:postgres
  postgres-tests
  (let [config (config/read-config (io/resource "config/test-postgres.clj"))]
    (config/configure config)
    (db-fixture invoke-model-test)
    (db-fixture model-lifecycle-test)
    (db-fixture model-interaction-test)
    (db-fixture model-link-test)
    (db-fixture parallel-include-test)
    (db-fixture localized-model-test)
    (db-fixture nested-model-test)
    (db-fixture fields-types-test)))
