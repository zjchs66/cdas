<template>
  <div class="container">
    <el-time-select
      v-if="pickerOptionType === '固定时间点'"
      v-model="value"
      :picker-options="pickerOptions"
      popper-class="select-time-container"
      :placeholder="placeholder"
      :style="{width: width ? width + 'px' : '194px', height: height ? height + 'px' : '32px'}">
    </el-time-select>

    <el-time-picker
      v-if="pickerOptionType === '任意时间点'"
      v-model="value"
      :picker-options="pickerOptions"
      popper-class="any-time-container"
      :placeholder="placeholder"
      :style="{width: width ? width + 'px' : '194px', height: height ? height + 'px' : '32px'}">
    </el-time-picker>

    <div class="fixed-time-range" v-if="pickerOptionType === '固定时间范围'">
      <el-time-select
        placeholder="起始时间"
        v-model="startTime"
        :picker-options="pickerOptions[0]"
        :style="{width: width ? width + 'px' : '194px', height: height ? height + 'px' : '32px'}">
      </el-time-select>
      <el-time-select
        placeholder="结束时间"
        v-model="endTime"
        :picker-options="pickerOptions[1]"
        :style="{width: width ? width + 'px' : '194px', height: height ? height + 'px' : '32px'}">
      </el-time-select>
    </div>

    <div class="any-time-range" v-if="pickerOptionType === '任意时间范围'">
      <el-time-picker
        is-range
        v-model="anyTimeRange"
        range-separator="至"
        start-placeholder="开始时间"
        end-placeholder="结束时间"
        placeholder="选择时间范围"
        popper-class="any-time-range-container"
        :style="{width: width ? width + 'px' : '436px', height: height ? height + 'px' : '32px'}">
      </el-time-picker>
    </div>
  </div>
</template>

<script>
module.exports = {
  data() {
    return {
      value: '',
      width: '',
      height: '',
      startTime: '',
      endTime: '',
      placeholder: '选择时间',
      pickerOptionType: '固定时间点',
      anyTimeRange: [new Date(2016, 9, 10, 8, 40), new Date(2016, 9, 10, 9, 40)],
    }
  },
  computed: {
    pickerOptions() {
      const fixedTime = {
        start: '08:30',
        step: '00:15',
        end: '18:30'
      }
      const anyTime = {
        selectableRange: '00:00:00 - 23:59:59'
      }
      const startTimeOptions = {
        start: '08:30',
        step: '00:15',
        end: '18:30'
      }
      const endTimeOptions = {
        start: '08:30',
        step: '00:15',
        end: '18:30',
        minTime: this.startTime
      }
      switch (this.pickerOptionType) {
        case '固定时间点':
          return fixedTime
        case '任意时间点':
          return anyTime
        case '固定时间范围':
          return [startTimeOptions, endTimeOptions]
        case '任意时间范围':
          return
      }
    }
  },
  methods:{
    init(self) {
      //初始化完成
    },
    property(self) {
      /**设计时属性面板列配置项**/
      return [
        // {label:'内容',key:'content',value:this.content||''}
        { label: '宽度', key: 'width', value: this.width || '', type: 'input' },
        { label: '高度', key: 'height', value: this.height || '', type: 'input' },
        { label: '默认文字', key: 'placeholder', value: this.placeholder, type: 'text' },
        { label: '类型', key: 'pickerOptionType', value: this.pickerOptionType, type: 'select', option: ['固定时间点', '任意时间点', '固定时间范围', '任意时间范围'] }
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
      }
      return null;
    },
  }
}
</script>
<style scoped>
.el-input__inner {
  height: 100%;
  line-height: 100%;
  border-radius: 2px;
}
/*光标聚焦 input 框时边框颜色*/
.el-input.is-active .el-input__inner, .el-input__inner:focus {
  border-color: #506eda;
}
.el-input__icon {
  line-height: 100%;
}
input[type="text"] {
  color: #000;
}
.el-input__prefix, .el-input__suffix {
  top: 1px;
}
.el-range-editor.el-input__inner {
  display: flex;
}
.el-date-editor .el-range-separator {
  height: auto;
}
.el-range-editor.is-active, .el-range-editor.is-active:hover {
  border-color: #506eda;
}
</style>
<style>
.select-time-container .time-select-item:hover {
  background-color: #e4e8fb;
  font-weight: normal;
}
.select-time-container .time-select-item.selected:not(.disabled) {
  color: #333;
  font-weight: normal;
}
.any-time-range-container.el-time-range-picker {
  width: 434px;
}
/*修改选中日期的背景色*/
.any-time-range-container .el-date-table td.current:not(.disabled) span {
  background-color: #4364bf;
}
/*修改今天日期颜色*/
.any-time-range-container .el-date-table td.today span {
  color: #4364bf;
}
.any-time-range-container .el-date-table td span {
  border-radius: 2px;
}
.any-time-range-container input[type="text"] {
  color: #000;
}
.any-time-range-container .time-select-item:hover {
  background-color: #e4e8fb;
  font-weight: normal;
}
.any-time-range-container .time-select-item.selected:not(.disabled) {
  color: #333;
  font-weight: normal;
}
.any-time-range-container .el-button--text {
  padding: 7px 15px;
  color: #4364bf;
}
.any-time-range-container .el-button {
  color: #4364bf;
  border-color: #4364bf;
}
.any-time-range-container .el-button.is-plain:focus, .el-button.is-plain:hover {
  color: #4364bf;
  border-color: #4364bf;
}
.any-time-range-container .el-date-table td.available:hover {
  color: #4364bf;
}
.any-time-range-container .el-input.is-active .el-input__inner, .el-input__inner:focus {
  border-color: #4364bf;
}
.any-time-range-container .el-time-panel__btn.confirm {
  color: #506eda;
}
.any-time-range-container button:hover {
  background: none;
}
.any-time-range-container .el-time-spinner__item:hover:not(.disabled):not(.active) {
  background-color: #e4e8fb;
}
</style>
