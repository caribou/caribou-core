(ns caribou.test.model
  (:use [caribou.debug]
        [caribou.model]
        [caribou.test]
        [clojure.test])
  (:require [clojure.java.jdbc :as sql]
            [clojure.java.io :as io]
            [caribou.db :as db]
            [caribou.auth :as auth]
            [caribou.util :as util]
            [caribou.logger :as log]
            [caribou.query :as query]
            [caribou.validation :as validation]
            [caribou.config :as config]
            [caribou.core :as core]))

(defn test-init
  []
  (invoke-models)
  (query/clear-queries))

(defn db-fixture
  [f config]
  (core/with-caribou config
    (test-init)
    (f)
    (doseq [slug [:yellow :purple :zap :chartreuse :fuchsia :base :level
                  :void :white :agent :pinkƒpink1]]
      (when (db/table? slug) (destroy :model (models slug :id)))
      (when (db/table? :everywhere)
        (do
          (db/do-sql "delete from locale")
          (destroy :model (models :nowhere :id))
          (destroy :model (models :everywhere :id))))
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
      (is (<= 8 (count (models :yellow :fields))))
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
                                   :link-slug "ibibib"}
                                  {:name "Yellows" :type "collection"
                                   :dependent true
                                   :target-id (yellow-row :id)}]})

        yellow (models :yellow)
        zap (models :zap)

        zzzap (create :zap {:ibibib "kkkkkkk"})
        yyy (create :yellow {:gogon "obobo" :wibib true :zap-id (zzzap :id)})
        yyyz (create :yellow {:gogon "igigi" :wibib false :zap-id (zzzap :id)})
        yy (create :yellow {:gogon "lalal" :wibib true :zap-id (zzzap :id)})]
    (update :yellow (yyy :id) {:gogon "binbin"})
    (update :zap (zzzap :id)
            {:ibibib "OOOOOO mmmmm   ZZZZZZZZZZ"
             :yellows [{:id (yyyz :id) :gogon "IIbbiiIIIbbibib"}
                       {:gogon "nonononononon"}]})

    (testing "Model interactions."
      (let [zap-reload (db/choose :zap (zzzap :id))]
        (is (= ((db/choose :yellow (yyyz :id)) :gogon) "IIbbiiIIIbbibib"))
        (is (= ((db/choose :yellow (yyy :id)) :gogon) "binbin"))
        (is (= (zap-reload :yobob) "oooooo-mmmmm-zzzzzzzzzz"))
        (is (= "OOOOOO mmmmm   ZZZZZZZZZZ"
               ((from zap zap-reload {:include {}})
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
          (is (= (purple :green-id) (zappo :id))))

        ;; testing the order function for collections
        (let [zap-query {:where {:id (:id zzzap)} :include {:yellows {}}}
              zappix (pick :zap zap-query)
              yellows (:yellows zappix)
              positions (reverse (range 1 (+ 1 (-> yellows count))))
              orderings (map
                         (fn [yellows pos]
                           {:id (:id yellows) :position pos})
                         (:yellows zappix) positions)]
          (order :zap (:id zzzap) :yellows orderings)
          (let [zapx (pick :zap zap-query)
                orders (map
                        (fn [yellows]
                          {:id (:id yellows) :position (:green-position yellows)})
                        (:yellows zapx))]
            (is (= (reverse positions) (map :green-position (:yellows zapx))))
            (is (= (sort-by :id orders) (sort-by :id orderings)))))

        (destroy :zap (:id zap-reload))
        (let [purples (util/query "select * from purple")]
          (is (empty? purples))))

      (destroy :model (zap :id))

      (is (empty? (models :purple :fields :green-id)))

      (destroy :model (models :purple :id))

      (is (and (not (db/table? :purple))
               (not (db/table? :yellow))
               (not (db/table? :zap)))))))

(defn collection-map-test
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
                                   :link-slug "ibibib"}
                                  {:name "Yellows" :type "collection"
                                   :map true
                                   :dependent true
                                   :target-id (yellow-row :id)}]})

        yellow (models :yellow)
        zap (models :zap)

        zzzap (create :zap {:ibibib "kkkkkkk"})
        yyy   (create :yellow {:gogon "obobo" :wibib true :zap-id (zzzap :id) :zap-key "grey"})
        yyyz  (create :yellow {:gogon "igigi" :wibib false :zap-id (zzzap :id) :zap-key "ochre"})
        yy    (create :yellow {:gogon "lalal" :wibib true :zap-id (zzzap :id) :zap-key "amarillo"})]
    (update :yellow (yyy :id) {:gogon "binbin"})
    (update :zap (zzzap :id)
            {:ibibib "OOOOOO mmmmm   ZZZZZZZZZZ"
             :yellows {:light {:id (yyyz :id) :gogon "IIbbiiIIIbbibib"}
                       :dark {:gogon "nonononononon"}}})

    (testing "Model interactions."
      (let [zap-reload (db/choose :zap (zzzap :id))]
        (is (= ((db/choose :yellow (yyyz :id)) :gogon) "IIbbiiIIIbbibib"))
        (is (= ((db/choose :yellow (yyy :id)) :gogon) "binbin"))
        (is (= (zap-reload :yobob) "oooooo-mmmmm-zzzzzzzzzz"))
        (is (= "OOOOOO mmmmm   ZZZZZZZZZZ"
               ((from zap zap-reload {:include {}})
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
          (is (= (purple :green-id) (zappo :id))))

        (destroy :zap (:id zap-reload))
        (let [purples (util/query "select * from purple")]
          (is (empty? purples))))

      (destroy :model (zap :id))

      (is (empty? (models :purple :fields :green-id)))

      (destroy :model (models :purple :id))

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
                           :target-id (chartreuse-row :id)}]})

        chartreuse (models :chartreuse)
        fuchsia (models :fuchsia)
        charfuch (models :chartreusii-fuchsia)

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
      (let [fff-X (from (models :fuchsia) fff
                        {:include {:chartreusii {}}})
            cec (pick :chartreuse {:where {:ondondon "ikikik"}
                                   :include {:fuchsia {}}})]
        (is (= 2 (count (retrieve-links cf-link ccc))))
        (is (= 2 (count (fff-X :chartreusii))))
        (is (some #(= % "granular") (map :zozoz (:fuchsia cec))))

        (update :model (:id chartreuse)
                {:fields [{:id (-> cf-link :row :id) :name "Nightpurple"
                           :slug "nightpurple"}]})

        ;; test ordering for links
        (let [charquery {:include {:nightpurple {}} :where {:id (:id ccc)}}
              ccoc (pick :chartreuse charquery)
              nightpurples (:nightpurple ccoc)
              positions (reverse (range 5 (-> nightpurples count (+ 5))))
              orderings (map
                         (fn [nightpurple pos]
                           (println "NIGHTPURPLE" nightpurple)
                           {:id (:id nightpurple) :position pos})
                         nightpurples positions)]
          (order :chartreuse (:id ccoc) :nightpurple orderings)
          (let [ccmc (pick :chartreuse {:include {:nightpurple-join {}} :where {:id (:id ccc)}})
                orders (map
                        (fn [nightpurple]
                          {:id (:nightpurple-id nightpurple) :position (:nightpurple-position nightpurple)})
                        (:nightpurple-join ccmc))]
            (is (= (sort-by :id orders) (sort-by :id orderings)))))

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
          (is (present? (models :chartreusii-nightpurple)))

          (let [falses (gather :chartreuse {:where {:kokok false}})]
            (doseq [fal falses]
              (log/out :FALSE fal))
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
                              :target-id (:id base-row)
                              :dependent true :reciprocal-name "Levels"}]})
        void-row (create
                  :model
                  {:name "Void"
                   :fields [{:name "Tether" :type "string"}
                            {:name "Base" :type "part" :target-id (:id base-row)
                             :dependent true :reciprocal-name "Void"}]})

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
            ;;                :invalid-field nil}}}))))))
            (validation/beams :base
                              {:include {:levels {} :void {}}
                               :where {:levels {:strata 5
                                                :invalid-field nil}}}))))))

(defn localized-model-test
  []
  (let [place (create :locale {:language "Ibeo" :region "Glass" :code "ib_or"})
        everywhere (create
                    :model
                    {:name "Everywhere"
                     :fields [{:name "Up" :type "string" :localized true}
                              {:name "Grass" :type "text" :localized true}
                              {:name "Through" :type "boolean" :localized true}
                              {:name "Form Structure" :type "asset" :localized true}
                              {:name "Colocate" :type "address" :localized true}
                              ;; {:name "Whentime" :type "timestamp"}
                              {:name "Under" :type "decimal" :localized true}]})
        other (create :locale {:language "Gornon" :region "Ipipip"
                               :code "go_xb"})
        nowhere (create
                 :model
                 {:name "Nowhere"
                  :fields [{:name "Down" :type "string" :localized true}
                           {:name "Everywhere" :type "link" :dependent true
                            :target-id (everywhere :id) :localized true}]})
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
                  {:fields [{:id (models :everywhere :fields :grass :row
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
         :everywhere-nowhere
         {:include {:everywhere {}}
          :where {:everywhere {:through true}}
          :order {:everywhere {:under :asc}}})

        bx-joins
        (gather
         :everywhere-nowhere
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

      (log/debug (form-uberquery
                  (models :everywhere)
                  {:include {:nowhere {}}
                   :where {:nowhere {:down "Ylel"}}
                   :order {:nowhere {:down :desc}}
                   :limit 3
                   :locale "xo_ub"})))))

(defn localized-map-field-test
  []
  (let [place (create :locale {:language "Ibeo" :region "Glass" :code "ib_or"})
        everywhere (create
                    :model
                    {:name "Everywhere"
                     :fields [{:name "Up" :type "string" :localized true}
                              {:name "Grass" :type "text" :localized true}
                              {:name "Through" :type "boolean" :localized true}
                              {:name "Form Structure" :type "asset" :localized true}
                              {:name "Colocate" :type "address" :localized true}
                              ;; {:name "Whentime" :type "timestamp" :localized true}
                              {:name "Under" :type "decimal" :localized true}]})
        other (create :locale {:language "Gornon" :region "Ipipip"
                               :code "go_xb"})
        nowhere (create
                 :model
                 {:name "Nowhere"
                  :fields [{:name "Down" :type "string" :localized true}
                           {:name "Everywhere" :type "link" :dependent true
                            :map true :target-id (everywhere :id) :localized true}]})
        a (create :everywhere {:up "Hey" :grass "On" :through true :under 10.1})
        b (create :everywhere {:up "What" :grass "Bead" :through true
                               :under 33.333})
        c (create :everywhere {:up "Is" :grass "Growth" :through false
                               :under 22222})
        xxx (create :nowhere {:down "Ylel" :everywhere {:yellow {:id (:id a)}}})
        outer (create :locale {:language "Obooe" :region "Xorxox"
                               :code "xo_ub"})
        other-other (update :locale (:id other) {:code "bx_pa"})

        _ (update
           :model (:id everywhere)
           {:fields [{:id (models :everywhere :fields :grass :row :id)
                      :name "Blade" :slug "blade"}]})

        xxx-other (update
                   :nowhere (:id xxx)
                   {:down "IiiiiiIIIIIII"}
                   {:locale "xo_ub"})

        xxx-other (update
                   :nowhere (:id xxx)
                   {:down "Prortrobr"
                    :everywhere
                    {:green {:id (:id b)}
                     :purple {:id (:id c)}}}
                   {:locale "bx_pa"})

        xxx-other (update
                   :nowhere (:id xxx)
                   {:down "Grungruublor"
                    :everywhere {:black {:id (:id b)}}}
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
         :everywhere-nowhere
         {:include {:everywhere {}}
          :where {:everywhere {:through true}}
          :order {:everywhere {:under :asc}}})

        bx-joins
        (gather
         :everywhere-nowhere
         {:include {:everywhere {}}
          :where {:everywhere {:through true}}
          :order {:everywhere {:under :asc}}
          :locale "bx_pa"})

        nowhat-query
        (form-uberquery
         (models :nowhere)
         {:where {:id (:id xxx)}
          :include {:everywhere {}}
          :locale "bx_pa"})

        nowhat
        (pick
         :nowhere
         {:where {:id (:id xxx)}
          :include {:everywhere {}}
          :locale "bx_pa"})

        everywhat
        (pick
         :everywhere
         {:where {:id (:id c)}
          :locale "xo_ub"})]

    (testing "Mapped field names"
      (is (= 1 (count xo-ub-eees)))
      (is (= 3 (count bx-pa-eees)))
      (is (= 1 (count ib-or-eees)))
      (is (= '("Growth" "Bead") (map :blade ordered-everywhere)))
      (is (= 1 (count joins)))
      (is (= "Hey" (-> joins first :everywhere :up)))
      (is (= "Prortrobr" (:down nowhat)))
      (is (= 3 (count (-> nowhat :everywhere))))
      (log/debug nowhat-query :NOWHATQUERY)
      (log/debug (-> nowhat :everywhere keys) :WHAT)
      (log/debug (-> nowhat :everywhere type) :WHAT)
      (doseq [non (-> nowhat :everywhere vals)]
        (log/debug (:up non) :MAP))
      (is (= "Is" (-> nowhat :everywhere :purple :up)))
      (is (= "Is" (:up everywhat)))
      (is (= 2 (count bx-joins)))
      (is (= '("Hey" "What") (map #(-> % :everywhere :up) bx-joins)))

      (log/debug (form-uberquery
                  (models :everywhere)
                  {:include {:nowhere {}}
                   :where {:nowhere {:down "Ylel"}}
                   :order {:nowhere {:down :desc}}
                   :limit 3
                   :locale "xo_ub"})))))

(defn model-abuse
  []
  (let [pink (create 
              :model 
              {:name "1PinkƒPink1"
               :fields [{:name "I1l0" :type "string"}
                        {:name "555f∆" :type "integer"}
                        {:name "555growth555" :type "decimal"}]})
        aaa (create :pinkƒpink1 {:i1l0 "YELLL" :f∆ 55555 :growth555 1.333338882202})
        results (gather :pinkƒpink1)]
    (log/debug aaa :AAA)
    (doseq [result results]
      (log/debug result :RESULT))))

(defn nested-model-test
  []
  ;; this is also implicitly verifying :tie fields
  (let [white (create :model {:name "White" :nested true
                              :fields [{:name "Grey" :type "string"}]})
        aaa (create :white {:grey "obobob"})
        bbb (create :white {:grey "ininin" :parent-id (aaa :id)})
        ccc (create :white {:grey "kkukku" :parent-id (aaa :id)})
        ddd (create :white {:grey "zezeze" :parent-id (bbb :id)})
        eee (create :white {:grey "omomom" :parent-id (ddd :id)})
        fff (create :white {:grey "mnomno" :parent-id (ddd :id)})
        ggg (create :white {:grey "jjijji" :parent-id (ccc :id)})
        tree (arrange-tree [aaa bbb ccc ddd eee fff ggg])]
    ;; fff-path (progenitors :white (fff :id))
    ;; bbb-children (descendents :white (bbb :id))]
    ;; (is (= 4 (count fff-path)))
    ;; (is (= 4 (count bbb-children))))
    (testing "Nested models."
      (doseq [branch tree]
        (log/debug (doall (str tree))))
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
                     ["born-at" "timestamp"]
                     ["dies-at" "timestamp"]
                     ["passport" "asset"]
                     ["license" "position"]
                     ["residence" "address"]])
        enum-field {:name "villian" :type "enum"
                    :enumerations [{:entry "Le Chiffre"}
                                   {:entry "Mr. Big"}
                                   {:entry "Sir Hugo Drax"}
                                   {:entry "Seraffimo Spang"}
                                   {:entry "General Grubozaboyschikov"}
                                   {:entry "Dr. Julius No"}
                                   {:entry "Auric Goldfinger"}]}
        fields (cons enum-field fields)
        agent (create
               :model
               {:name "Agent"
                :fields fields})

        passport (create :asset {:filename "passport.picture"})
        dopple-passport (create :asset {:filename "passport.picture"})
        bond-height 69.55
        bond-values {:nchildren 32767
                     :height bond-height
                     :name (str "James Bond" (util/rand-str 25))
                     :auth "Octopu55y"
                     :round ".38 Hollow Tip"
                     ;; mysql text type size issue
                     :bio (str "He did stuff: ☃" (util/rand-str 65))
                     ;; :bio (str "He did stuff: ☃" (util/rand-str 65500))
                     :adequate true
                     ;; mysql date-time precision issue
                     :born-at "January 1 1970"
                     ;; mysql date-time precision issue
                     :dies-at "January 1 2037"
                     :passport-id (:id passport)
                     :villian "Auric Goldfinger"
                     ;; connectivity issues prevent this from working
                     ;; :residence {:address "WHITE HOUSE"
                     ;;             :country "USA"}
                     }
        bond (create :agent bond-values)
        doppleganger (create :agent (assoc bond-values
                                      :born-at "January 2 1970"))
        agent-id (:id agent)
        bond-id (:id bond)
        doppleganger-id (:id doppleganger)
        bond (pick :agent {:where {:id bond-id}})
        dopple (following :agent bond :born-at)
        ;; amount of floating point error that is allowable
        ;; mysql precision issue
        EPSILON 0.000001
        run-field-tests
        (fn []
          (let [brosnan (create :agent {:license 8
                                        :bio "A great man"
                                        :name "Pierce Brosnan"})
                connory (create :agent {:license 7})
                moore (create :agent {})]
            (testing "valid return in model/create"
              (is (= (:bio brosnan) "A great man"))
              (is (= (:name brosnan) "Pierce Brosnan")))
            (testing "Valid properties of field types."
              (is (seq agent))
              (is (seq bond))
              (is (seq dopple))
              ;; id fields are implicit
              (is (= -1 (.compareTo (:born-at bond) (:born-at dopple))))
              (is (number? (:id agent)))
              (is (number? (:id bond)))
              (is (number? (:id dopple)))
              (is (= (:nchildren bond) 32767))
              (is (= "Auric Goldfinger" (:villian bond)))
              (is (< (java.lang.Math/abs (- bond-height (:height bond))) EPSILON))
              ;; case sensitivity
              (is (= "James Bond" (subs (:name bond) 0 10)))
              (is (= (count (:name bond)) 35))
              (is (not (= (:auth bond) "Octopu55y")))
              (is (auth/check-password "Octopu55y" (:crypted-auth bond)))
              ;; (is (= "_8_hollow_tip" (:round bond)))
              ;; unicode
              (is (= "He did stuff: ☃" (subs (:bio bond) 0 15)))
              (is (= (count (:bio bond)) 80))
              ;; if this breaks...
              (is (:adequate bond))
              ;; unix time of 1/1/1970 / 1/1/2037
              (is (and (= 28800000 (.getTime (:born-at bond)))
                       (= 2114409600000 (.getTime (:dies-at bond)))))
              (is (= "passport.picture" (-> bond :passport :filename)))
              ;; (is (and (= 0.0 (-> bond :residence :lat))
              ;;          (= 0.0 (-> bond :residence :long))))
              (is (not= (:license bond) (:license dopple)))
              (is (number? (:license bond)))
              (is (#{0 1} (:license bond)))
              (is (#{0 1} (:license dopple)))
              (is (= 7 (:license connory)))
              (is (= 8 (:license brosnan)))
              (is (#{9 10 11} (:license moore)))
              (destroy :agent (:id brosnan))
              (destroy :agent (:id connory))
              (destroy :agent (:id moore)))))]
    (run-field-tests)
    (testing "Deletion of fields."
      (is (nil?
           (let [field-slugs (map (comp keyword :name) fields)
                 field-ids (map
                            (fn [slug]
                              (models :agent :fields slug :row :id))
                            field-slugs)
                 field-ids (filter number? field-ids)]
             (log/debug (str "field slugs" (apply str field-slugs)))
             (log/debug (str "field ids" (apply str field-ids)))
             (doseq [id field-ids]
               (destroy :field id))))))
    (testing "Addition of fields to a model."
      (is (nil? (doseq [field-spec fields]
                  (create :field (merge field-spec {:model-id agent-id}))))))
    (testing "Updating field values."
      (is (nil? (doseq [update-spec bond-values]
                  (update :agent bond-id (into {} [update-spec]))))))
    (run-field-tests)))

(defn all-model-tests
  [config]
  (db-fixture model-abuse config))
  ;; (db-fixture invoke-model-test config)
  ;; (db-fixture model-lifecycle-test config)
  ;; (db-fixture model-interaction-test config)
  ;; (db-fixture model-link-test config)
  ;; (db-fixture parallel-include-test config)
  ;; (db-fixture localized-model-test config)
  ;; (db-fixture nested-model-test config)
  ;; (db-fixture fields-types-test config)
  ;; (db-fixture collection-map-test config)
  ;; (db-fixture localized-map-field-test config))

(deftest ^:mysql
  mysql-tests
  (let [config (read-config :mysql)]
    (all-model-tests config)))

(deftest ^:postgres
  postgres-tests
  (let [config (read-config :postgres)]
    (all-model-tests config)))

(deftest ^:h2
  h2-tests
  (let [config (read-config :h2)]
    (all-model-tests config)))
