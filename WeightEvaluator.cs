using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mozart.SeePlan.DataModel;
using Mozart.Extensions;
using Mozart.Simulation.Engine;
using Mozart.Task.Execution;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Mozart.SeePlan.Simulation
{
    /// <summary>
    /// Weight Factor 기반의 Dispatcher를 사용할 경우 이 객체를 이용하여 Weight계산
    /// </summary>
    
    /// <summary>
    /// Calculates the weight score using this instance in case the equipment is using weight factor based dispatcher for dispatching. 
    /// </summary>
    public class WeightEvaluator
    {
        ActiveObject target;
        IComparer<IHandlingBatch> comparer;
        IList<IWeightMethod> factorList;
        WeightPreset preset;


        /// <summary>
        /// Target equipment object to perform Dispatching. 
        /// </summary>
        public ActiveObject Target
        {
            get { return this.target; }
        }

        /// <summary>
        /// Weight Factor의 집합
        /// </summary>
        
        /// <summary>
        /// Represents the set of weight factors. 
        /// </summary>
        public WeightPreset Preset
        {
            get
            {
                if (this.preset != null)
                    return this.preset;
                else if (this.target != null && this.target is IWeightPresetProvider)
                    return (this.target as IWeightPresetProvider).Preset;

                return null;
            }
        }

        /// <summary>
        /// The root Simulation Model(Factory) object.
        /// </summary>
        public AoFactory Factory
        {
            get { return AoFactory.Current; }
        }

        /// <summary>
        /// Model Logger.
        /// </summary>
        public IModelLog Log
        {
            get { return this.Factory.Logger; }
        }

        /// <summary>
        /// Comparer used by Dispatcher to evaluate waiting WIPs .
        /// </summary>
        public IComparer<IHandlingBatch> Comparer
        {
            get
            {
                if (this.comparer == null)
                    return System.Collections.Generic.Comparer<IHandlingBatch>.Default;
                return this.comparer;
            }
            set { this.comparer = value; }
        }

        /// <summary>
        /// The list of functions used by Weight based Dispatcher to calculate Factor value. 
        /// </summary>
        public IList<IWeightMethod> FactorList
        {
            get
            {
                if (this.factorList == null)
                    this.InitPreset();
                return this.factorList;
            }
        }

        /// <summary>
        /// 지정된 ActiveObject와 WeightPreset을 사용하여 새로운 WeightEvaluator를 생성합니다.
        /// </summary>
        /// <param name="target">Weight 기반 작업물 우선순위 결정이 필요한 ActiveObject입니다.</param>
        /// <param name="preset">Weight 정보를 포함하고 있는 WeightPreset입니다.</param>
       
        /// <summary>
        /// Initializes a new instance of the <em>WeightEvaluator</em> class with the specified <em>ActiveObject</em> and <em>WeightPreset</em>
        /// </summary>
        /// <param name="target">The active object which requires the priority decision of the weight factor based entity.</param>
        /// <param name="preset">The weight preset containing the weight factor information.</param>
        public WeightEvaluator(ActiveObject target, WeightPreset preset = null)
        {
            this.target = target;
            this.preset = preset;
        }

        static List<IWeightMethod> emptyFactorList = new List<IWeightMethod>(4);
        private void InitPreset()
        {
            if (this.preset == null && (this.target == null || !(this.target is IWeightPresetProvider)))
            {
                ModelContext.Current.ErrorLister.Write(DataActions.ErrorType.Error, Strings.CAT_SIM_INIT,
                    "provide WeightPreset or The target of WeightEvaluator must implement IWeightPresetProvider");

                this.factorList = emptyFactorList;
                return;
            }

            this.BuildPreset(this.Preset);
        }

        private void BuildPreset(WeightPreset preset)
        {
            if (preset == null || preset.FactorList.Count == 0)
            {
                throw new InvalidOperationException(string.Format(Strings.EXCEPTION_INVALID_PRESET_SETTING, preset.Name));
            }

            var factory = this.Factory;
            if (factory.Weights == null)
            {

                ModelContext.Current.ErrorLister.Write("NotRegisteredWeightMethod", DataActions.ErrorType.Error, Strings.CAT_SIM_FACTORY,
                    Strings.ERROR_NOT_REGISTERED_WEIGHTMETHODS);
                return;
            }

            this.factorList = factory.Weights.GetMethods(preset.Name).ToList();
        }

        /// <summary>
        /// WeightMethod 를 이용하여 Weight Factor에 대한 평가 결과 값을 산출 후 값을 반환
        /// </summary>
        /// <param name="factor">작업물의 우선순위를 평가하기 위한 평가요소</param>
        /// <param name="method">Weight Factor 에 대한 평가 결과 값을 산출하기 위한 함수</param>
        /// <param name="hb">평가 대상 작업물입니다.</param>
        /// <param name="ctx">작업물 평가에 사용되는 컨텍스트 개체입니다.</param>
        /// <returns>작업물의 평가 요소에 대한 결과값입니다.</returns>
        
        /// <summary>
        /// Calculates and returns the <em>WeightValue</em> of the weight factor defined by <em>WeightMethod</em> delegate. 
        /// </summary>
        /// <param name="factor">The factor reference to calculate <em>WeightValue</em> to be used to evaluate dispatch priority of the specified entity.</param>
        /// <param name="method">The delegate that defines the formula to calculate the <em>WeightValue</em> for the specified weight factor.</param>
        /// <param name="hb">The entity to receive <em>WeightValue</em> to be used for dispatch order evaluation.</param>
        /// <param name="ctx">The context object used to evaluate the dispatch order of the specified entity. </param>
        /// <returns>The <em>WeightValue</em> calculated from the defined <em>method</em> for the <em>factor</em>, to sort the dispatch priority of the specified <em>hb</em>.</returns>
        public WeightValue GetWeight(WeightFactor factor, WeightMethod method, IHandlingBatch hb, IDispatchContext ctx)
        {
            var score = method.Invoke(hb, this.Factory.NowDT, this.target, factor, ctx);
            score.Factor = factor;

            return score;
        }

        /// <summary>
        /// 작업물별 WeightFactor의 WeightValue를 평가하여 우선순위를 반영한 결과 반환
        /// </summary>
        /// <param name="lots">평가 대상 작업물의 목록입니다.</param>
        /// <param name="ctx">작업물 평가에 사용되는 컨텍스트 개체입니다.</param>
        /// <returns>우선순위를 고려해서 정렬된 작업물의 목록입니다.</returns>
        
        /// <summary>
        /// Returns sorted list of entities by dispatch order(Ascending), by evaluating the <em>WeightValue</em> of the <em>WeightFactor</em> per entity in the specified list of entities.
        /// </summary>
        /// <param name="lots">The list of entities to evaluate dispatch order.</param>
        /// <param name="ctx">The context object used to evaluate the dispatch order of the specified entity.</param>
        /// <returns>The sorted list of entities in dispatch order(Ascending).</returns>
        public IList<IHandlingBatch> Evaluate(IList<IHandlingBatch> lots, IDispatchContext ctx)
        {
            if (this.Comparer == null)
                return lots;
            if (this.FactorList.Count == 0)
                return lots;

            var stepDic = new ConcurrentDictionary<Step, WeightInfo>();

            if (SeeplanConfiguration.Instance.EnableThreadedWeightEvaluation)
            {
                var list = lots.ToList();
                Parallel.ForEach(list, (hb) => EvaluateOne(stepDic, hb, ctx));
                ParallelAlgorithms.Sort(list, this.Comparer);
                return list;
            }
            else
            {
                var list = new List<IHandlingBatch>(lots.Count);
                foreach (var hb in lots)
                {
                    EvaluateOne(stepDic, hb, ctx); 
                    list.AddSort(hb, this.comparer);
                }
                return list;
            }
        }

        private void EvaluateOne(ConcurrentDictionary<Step, WeightInfo> dict, IHandlingBatch hb, IDispatchContext ctx)
        {
            if (hb.Sample == null)
                return;

            if (this.FactorList == null)
                return;

            var lot = hb.Sample;
            var isGroup = hb is LotGroup<ILot, Step>;

            var lotInfo = lot.WeightInfo;
            var stepInfo = dict.GetOrAdd(lot.CurrentStep, (s) => new WeightInfo());

            lotInfo.Reset();

            foreach (var info in this.FactorList)
            {
                WeightValue wval = null;

                var factor = info.Factor;
                if (factor.Type == FactorType.FIXED)
                {
                    wval = lotInfo.GetFixedValueData(factor);
                    if (wval.IsMinValue)
                        wval = this.GetWeight(factor, info.Method, lot, ctx);
                }
                else if (factor.Type == FactorType.STEPTYPE)
                {
                    lock (stepInfo)
                    {
                        wval = stepInfo.GetValueData(factor);
                        if (wval.IsMinValue)
                            wval = this.GetWeight(factor, info.Method, lot, ctx);

                        stepInfo.SetValueData(factor, wval);
                    }
                }
                else
                {
                    wval = this.GetWeight(factor, info.Method, lot, ctx);
                }

                if (target is AoEquipment aeqp)

                    //Factory.WeightOptimizer.SetInputFeatureValue(aeqp.EqpID, factor, hb, wval.RawValue);

                    lotInfo.SetValueData(factor, wval);
            }
        }

        /// <summary>
        /// LogGroup 내의 개별 작업물을 정렬한 목록을 반환합니다.
        /// </summary>
        /// <param name="list">LotGroup에 속한 작업물의 목록입니다.</param>
        /// <param name="ctx">작업물 평가에 사용되는 컨텍스트 개체입니다.</param>
        /// <returns>우선순위를 고려해서 정렬된 작업물의 목록입니다.</returns>

        /// <summary>
        /// Returns the sorted list of entities in the <em>LotGroup</em>.
        /// </summary>
        /// <param name="list">The list of entities in the <em>LotGroup</em>.</param>
        /// <param name="ctx">The context object used to evaluate the dispatch order of the specified entity.</param>
        /// <returns>The sorted list of entities in dispatch order (Ascending).</returns>
        public IList<IHandlingBatch> SortLotGroupContents(IList<IHandlingBatch> list, IDispatchContext ctx)
        {
            if (this.FactorList.Count == 0)
                return list;

            var result = new List<IHandlingBatch>(list.Count);

            foreach (IHandlingBatch hb in list)
            {
                var lot = hb.Sample;
                var lotInfo = lot.WeightInfo;

                lotInfo.Reset();

                foreach (var info in this.FactorList)
                {
                    WeightValue wval = null;
                    WeightFactor factor = info.Factor;

                    if (factor.Type != FactorType.LOTTYPE)
                        continue;

                    wval = this.GetWeight(factor, info.Method, lot, ctx);

                    lotInfo.SetValueData(factor, wval);
                }

                result.AddSort(hb, this.Comparer);
            }

            return result;
        }
    }
}
