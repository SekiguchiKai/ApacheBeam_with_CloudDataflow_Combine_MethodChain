package com.company;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;


/**
 * メインクラス
 * Created by sekiguchikai on 2017/07/13.
 */
public class Main {
    /**
     * 関数型オブジェクト
     * String => Integerの型変換を行う
     */
    static class TransformTypeFromStringToInteger extends DoFn<String, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            // 要素をString=>Integerに変換して、output
            c.output(Integer.parseInt(c.element()));
        }
    }

    /**
     * 関数型オブジェクト
     * Integer =>Stringの型変換を行う
     */
    static class TransformTypeFromIntegerToString extends DoFn<Integer, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            // 要素をString=>Integerに変換して、output
            System.out.println(c.element());
            c.output(String.valueOf(c.element()));
        }
    }


    /**
     * インプットデータのパス
     */
    private static final String INPUT_FILE_PATH = "./sample.txt";
    /**
     * アウトデータのパス
     */
    private static final String OUTPUT_FILE_PATH = "./result.txt";

    /**
     * メインメソッド
     *
     * @param args
     */
    public static void main(String[] args) {
        // Pipeline生成
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        // 処理部分
        pipeline.apply(TextIO.read().from(INPUT_FILE_PATH))
                .apply(ParDo.of(new TransformTypeFromStringToInteger()))
                .apply(Sum.integersGlobally().withoutDefaults())
                .apply(ParDo.of(new TransformTypeFromIntegerToString()))
                .apply(TextIO.write().to(OUTPUT_FILE_PATH));

        // 実行
        pipeline.run().waitUntilFinish();
    }
}