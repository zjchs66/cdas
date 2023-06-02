<template>
  <div class="el-radio-detail-container" v-if='rootDisplay'>
    <div v-if="!display1">
      <el-radio v-model="value"  v-for="v in labels" :label="v" :key="v" size="small"  @change="onblur" :disabled="isEnabled">{{v}}</el-radio>
      <el-input  v-model="inputValue"  style="margin-top:5px;margin-bottom:5px;width:10%" :disabled="isEnabled"></el-input>
      <el-button @click="summit" type="primary" :disabled="isEnabled">{{buttonName}}</el-button>
    </div>
    <div v-else>
        {{inputValue}}
    </div>
  </div>
</template>

<script>
    module.exports = {
        data() {
            return {
                labels:['0.3','0.5','0.8','1'],
                value:'',
                from:'form',
                callbackFunc:'',
                initCallbackFunc:'',
                inputValue:'',
                buttonName:'确认',
                item:'',
                display1:null,
                isEnabled:false,
                rootDisplay:true
            }
        },
        methods:{
            init(self) {
              //初始化完成
            },
            property(self) {
                /**设计时属性面板列配置项**/
                return [
                    {label: '是否root启用', key: 'isRootDisplay', value: this.isRootDisplay, type: 'checkbox'},
                    {label: '默认值', key: 'value', value: this.value, type: 'textarea'},
                    {label: '初始化回调方法', key: 'initCallbackFunc', value: this.initCallbackFunc, type: 'function'},
                    {label: '提交回调方法', key: 'callbackFunc', value: this.callbackFunc, type: 'function'},
                    {label:'选项',key:'labels',value:this.labels,type:'textarea'},
                    {label:'按钮名称',key:'buttonName',value:this.buttonName,type:'textarea'},
                    // {label: '回调方法', key: 'callbackFunc', value: this.callbackFunc, type: 'function'}
                ];
            },

            refresh(self) {
                //设计时属性值变化刷新动作
            },

            render(self,data,option) {
                /**运行时渲染**/
                // todo this.runtime(element) or null
                if(option['item'] !== undefined){
                    this.item = option['item']
                }
                if(this.initCallbackFunc){
                  var data1 = window[this.initCallbackFunc](this.item);
                  console.log('dataaaaa',data1)
                  this.val(this,data1)
              }
            
            },
            val(self,data) {
                /**运行时获取表单数据,如果有data则填充表单数据*/
                if(data!==undefined&&data!=null){
                    //todo
                    console.log(data,typeof data);
                    this.display1 = data;
                    this.inputValue = data;
                }else{
                    this.inputValue = this.value
                }
                return this.inputValue;
            },
            onblur(v){
                this.inputValue = v
            //     if (this.callbackFunc){
            //         window[this.callbackFunc](this.value)
            //     }
             },
            summit(v){
                console.log('pp1',v)
                    if (this.callbackFunc){
                    window[this.callbackFunc](this.item,this.inputValue)
                }
            }
             
        }
    }
</script>
<style scoped>
/*.el-radio-detail-container .el-radio__input.is-checked+span.el-radio__label {*/
/*  color: #506eda;*/
/*  cursor: default;*/
/*}*/
/*.el-radio-detail-container .el-radio__input.is-disabled.is-checked+span.el-radio__label {*/
/*  color: #506eda;*/
/*  cursor: default;*/
/*}*/
/*.el-radio-detail-container .el-radio__input.is-disabled+span.el-radio__label {*/
/*  color: #333;*/
/*  cursor: default;*/
/*}*/
/*.el-radio-detail-container .el-radio__input .el-radio__inner {*/
/*  cursor: default;*/
/*}*/
/*.el-radio-detail-container .el-radio__input.is-checked .el-radio__inner {*/
/*  border-color: #506eda;*/
/*  background: #506eda;*/
/*}*/
</style>
