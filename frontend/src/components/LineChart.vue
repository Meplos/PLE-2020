<script>
import { Line } from 'vue-chartjs';

export default {
    extends : Line,
    props: {
        label: {
            type: String
        },
        chartData: {
            type: Array
        },
        options: {
            type: Object
        },
        chartColors: {
            type: Object
        }
    },
    mounted() {
        const dates = this.chartData.map(chart => chart.date);
        const value = this.chartData.map(chart => chart.total);

        const {borderColor, pointBorderColor, pointBackgroundColor, backgroundColor } = this.chartColors;

        this.renderChart({
            labels: dates,
            datasets : [{
                label: this.label,
                data: value,
                borderColor: borderColor,
                pointBorderColor: pointBorderColor,
                pointBackgroundColor: pointBackgroundColor,
                backgroundColor: backgroundColor,
            }],
        },
        this.options
        );
    }
}
</script>