import { useEffect, useState } from 'react'
import amsterdam from '../assets/cities/Amsterdam.png'
import doneImage from '../assets/check-circle-outline.svg'
import progressImage from '../assets/timer-sand.svg'
import './App.css'
import Header from '../components/Header'
import Footer from '../components/Footer'

function App() {

  const cities = ['Amsterdam', 'Brussels', 'Copenhagen', 'Helsinki']
  const [cityIndex, setCityIndex] = useState(0)
  const [cityImage, setCityImage] = useState(amsterdam)
  const [imgLoading, setImgLoading] = useState('entering')
  const [citiesData, setCitiesData] = useState({})
  import(`../assets/data/cities.json`).then(data => setCitiesData(data.default))


  useEffect(() => {
    setTimeout(() => {
      const newCityIndex = (cityIndex + 1) % (cities.length - 1)
      setCityIndex(newCityIndex)
      setImgLoading('leaving')
      setTimeout(() => 
        import(`../assets/cities/${cities[newCityIndex]}.png`).then(image => {
          setCityImage(image.default);
          setTimeout(() => setImgLoading('entering'), 100)
          console.log(newCityIndex)
        }), 100
      )
    }, 5000)
  
  }, [cityIndex]);

  return (
    <>
      <Header />
      <div className='container heroes'>
        <div className='heroHeader'>
          <h1>Check how <span className={`coloredCity animatedCityWidth animated ${imgLoading}`}>{cities[cityIndex]}</span> scores on mobility equity.</h1>
          <div className='coloredCityContent'>
            <p>Mobility offers us options, opportunities and general wellbeing. DUTT allows a comparison between neighbourhoods and 
              whole cities to understand how well they move you, your neighbours, and everyone around you. Understand
              which groups are privileged, and which groups are under-performant. Discover the power of walking,
              biking, trains, transit, and cars. All for free.</p>

            <p>We are currently finalising building of the website and interactive parts. Sign up for the 
            waiting list to be alerted whenever we go live. </p>
            <a href="mailto:ivo@urbanvind.com?subject=Waiting%20List%20Urban%20Transit%20Times&body=To%20make%20sure%20we%20have%20info%20relevant%20for%20you%2C%20add%20your%20nearest%20biggest%20city%2C%20like%20Amsterdam%2C%20Paris%2C%20Barcelona.%20You%20can%20also%20add%20your%20occupation%20(planner%2C%20student%2C%20etc.)%2C%20so%20we%20get%20to%20know%20each%20other%20a%20little%20bit%20%3A)%20We%20will%20send%20you%20a%20message%20when%20we%20are%20live%2C%20and%20send%20you%20newsletters%20if%20you%20indicate%20you%20want%20it.%0D%0A%0D%0ACity%3A%0D%0AMy%20role%3A%0D%0ANewsletter%20(yes%2Fno)%3A">
              <div className='button'>SIGN UP FOR WAITING LIST</div>
            </a>
          </div>
        </div>
        <div className='cityMap'>
          <img src={cityImage} className={`cityMapImage animated ${imgLoading}`} />
          <div className='cityImageText'>Increase in accessibility by supplementing bike with public transit in {cities[cityIndex]}.</div>
        </div>
      </div>

      <div className='container'>
        <div className='cityListContainer'>
          <h1><span className={`coloredCity`}>Cities</span> included</h1>
          <p>Based on open source mapping data from OpenStreetMap and public transit data 
            from TransitLand, we are able to create accessibility statistics for many cities, such as the following. </p>
          {Object.keys(citiesData).map((key) => <>
            <h3>{key}</h3>
            {citiesData[key].map(({city_name, frac_req_ok}) => <div className='cityListItem'>
              {city_name}
              <img src={frac_req_ok == 1.0 ? doneImage : progressImage} className='cityListIndicator' />
            </div>)}
          </>)}
        </div>
      </div>
      <Footer />
    </>
  )
}

export default App
