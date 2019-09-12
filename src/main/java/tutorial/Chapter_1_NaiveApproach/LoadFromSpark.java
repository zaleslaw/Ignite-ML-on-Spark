package tutorial.Chapter_1_NaiveApproach;

import Utils.TitanicUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

public class LoadFromSpark {
    /** Run example. */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start()) {
            IgniteCache<Integer, Object[]> dataCache = TitanicUtils.loadFromSpark(ignite);

            IgniteBiFunction<Integer, Object[], Vector> featureExtractor = (k, v) -> {
                double[] data = new double[] {
                    Double.parseDouble((String)v[0]),
                    v[5] == null ? 0 : Double.parseDouble((String)v[5]),
                    v[6] == null ? 0 : Double.parseDouble((String)v[6])
                };

                return VectorUtils.of(data);
            };

            IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> Double.parseDouble((String)v[1]);

            DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(5, 0);

            DecisionTreeNode mdl = trainer.fit(
                ignite,
                dataCache,
                featureExtractor, // "pclass", "sibsp", "parch"
                lbExtractor
            );

            System.out.println("\n>>> Trained model: " + mdl);

            double accuracy = Evaluator.evaluate(
                dataCache,
                mdl,
                featureExtractor,
                lbExtractor,
                new Accuracy<>()
            );

            System.out.println("\n>>> Accuracy " + accuracy);
            System.out.println("\n>>> Test Error " + (1 - accuracy));
        }
    }
}
