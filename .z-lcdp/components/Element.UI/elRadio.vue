<template>
  <div class="el-radio-detail-container">
    <div v-if="from=='form'">
      <el-radio v-model="value"  v-for="v in labels" :label="v" :key="v" size="small"  @change="onblur">{{v}}</el-radio>
    </div>
    <div v-else>
      <el-radio v-model="value" v-for="v in labels" :label="v" :key="v" size="small" :disabled="true">{{v}}</el-radio>
    </div>
  </div>
</template>

<script>
    module.exports = {
        data() {
            return {
                labels:['选项一','选项二','选项三'],
                value:'',
                from:'form',
                callbackFunc:''
            }
        },
        methods:{
            init(self) {
              //初始化完成
              this.from = self.from;
            },
            property(self) {
                /**设计时属性面板列配置项**/
                return [
                    {label: '回调方法', key: 'callbackFunc', value: this.callbackFunc, type: 'function'},
                    {label:'选项',key:'labels',value:this.labels,type:'textarea'}
                    // {label: '回调方法', key: 'callbackFunc', value: this.callbackFunc, type: 'function'}
                ];
            },

            refresh(self) {
                //设计时属性值变化刷新动作
            },

            render(self,data,option) {
                /**运行时渲染**/
                // todo this.runtime(element) or null
            },
            val(self,data) {
                /**运行时获取表单数据,如果有data则填充表单数据*/
                if(data!==undefined&&data!=null){
                    //todo
                    console.log(data,typeof data);
                    this.value = data;
                }
                return this.value;
            },
            onblur(v){
                if (this.callbackFunc){
                    window[this.callbackFunc](this.value)
                }
            }
        }
    }
</script>
<style scoped>
.el-radio-detail-container .el-radio__input.is-checked+span.el-radio__label {
  color: #506eda;
  cursor: default;
}
.el-radio-detail-container .el-radio__input.is-disabled.is-checked+span.el-radio__label {
  color: #506eda;
  cursor: default;
}
.el-radio-detail-container .el-radio__input.is-disabled+span.el-radio__label {
  color: #333;
  cursor: default;
}
.el-radio-detail-container .el-radio__input .el-radio__inner {
  cursor: default;
}
.el-radio-detail-container .el-radio__input.is-checked .el-radio__inner {
  border-color: #506eda;
  background: #506eda;
}
</style>
