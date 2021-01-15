<template>
    <v-container>
      <v-row
      justify="center"
      class="mt-10">
        <h1>Langage Evolution</h1>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
        <v-btn class="mr-5"
          rounded
          id="fr"
          color="primary"
          @click="wichButton"
          dark
        >
          fr
        </v-btn>
        <v-btn class="mr-5"
          rounded
          id="en"
          color="primary"
          @click="wichButton"
          dark
        >
          en
        </v-btn>
         <v-btn class="mr-5"
          rounded
          id="es"
          color="primary"
          @click="wichButton"
          dark
        >
          es
        </v-btn>
        <v-btn class="mr-5"
          rounded
          id="ja"
          color="primary"
          @click="wichButton"
          dark
        >
          ja
        </v-btn>
      </v-row>
      <v-row
      justify="center"
      class="mt-10">
      <div v-if="langEvolutionData.length >0" >
        <line-chart :chartData="langEvolutionData" :chartColors="chartColors" :options="chartOptions" :height="600" :width="900" :label="label"></line-chart>
      </div>
      </v-row>
    </v-container>
</template>

<script>
import axios from 'axios';
import LineChart from '../components/LineChart';

export default {
  name: 'LangEvolution',
  components: {
    LineChart
  },
  data: () => ({
   langEvolutionData: [],
    chartColors: {
      borderColor: "rgba(30, 30, 250, 1)",
      pointBorderColor: "#656176",
      pointBackgroundColor: "#F8F1FF",
      backgroundColor: "rgba(43, 43, 231, 0.4)",
    },
    chartOptions: {
      responsive: true,
    },
    label: "",
  }),
  methods: {
    async wichButton(){
      let targetId = event.currentTarget.id;
      this.label = targetId;
      const response = await axios("http://localhost:7000/language_pop/"+targetId);
      const days = response.data.map(day => {
        let col = day.column.split(":")
        return col[1];
      });
      const value = response.data.map(day => day.$);
      const result = [];
      for (let index = 0; index < days.length-1; index++) {
        result.push({date: days[index], total: value[index]})
      }
      console.log(result);
      this.langEvolutionData = [];
      await this.timeout(50);
      this.langEvolutionData = result;
    },
    async timeout(ms) {
      return new Promise(resolve => setTimeout(resolve, ms));
    }
  },
}
</script>
