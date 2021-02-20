# Effective Reproduction Time (Rt) 

## Introduction:

The *basic reproduction number, R,<sub>0</sub>* is useful to understand the transmissiblity of disease and design various intervention strategies<sup>1</sup>. The *R<sub>0<sub>* allows us to develop surveillance and intervention strategies. For a new epidemic, all people as susceptible to the disease, hence the *R<sub>0</sub>* is an appropriate measure of the disease. However, as time progresses, there is a change in the susceptiblity to disease. The following reasons are some due to which this change may occur:-

* Intrinsic factors:-

    * People contracting and recovering from the disease may now be immune to re-infection.
    * The infecting organism may mutate making it either less or more virulent.

* Extrinsic factors:-

    * Prevention strategies like masking, social distancing are practiced and adopted by society at large.
    * Pharmacologic prophylaxis may make infection less likely.

Given these factors, rather than *R<sub>0</sub>* , the effective reproduction number, abbreviated as *R<sub>t</sub>* is a more appropriate time-dependent representation of the course of an epidemic. 

##  Understanding the Effective reproduction number R<sub>t</sub>:

The effective reproduction number is the actual average number of cases that each infection individual infects during the course of the epidemic at that time period. The *R<sub>t</sub>* can provide an overview of the course trajectory of the epidemic:-

* If the calculated R<sub>t</sub> > 1, then this suggests that the epidemic is increasing.

## Method of *R<sub>t</sub>*:

The R<sub>t</sub> has been calculated using the EpiEstim 2.2.4 package in R (The R foundation for Statistical Computing, Austria) <sup>2</sup>. This package calculates the *instantaneous reproduction number* using the method described by Cori et al<sup>3</sup>. The method used by us is a non-parametric serial interval distribution. We used a 7-day sliding window for calculation. The parameters used in our model are derived by the analysis performed by a group of researchers reponsible for the *Covid-Today* portal <sup>4</sup>. The parameters inputted into the model were obtained from a study of 468 patients in China<sup>5</sup>. Results of *R<sub>t</sub>* are presented with their 95% confidence intervals.

## Limitations:

* The *R<sub>t</sub>* was calculated based on reported cases obtained from health authorities. Changes in testing and reporting will affect positivity rates and hence *R<sub>t</sub>* calculation.
* Lack of local serial interval estimation may alter calculations.


## References:

1. Nishiura, H., & Chowell, G. (2009). The Effective Reproduction Number as a Prelude to Statistical Estimation of Time-Dependent Epidemic Trends. Mathematical and Statistical Estimation Approaches in Epidemiology, 103–121. https://doi.org/10.1007/978-90-481-2313-1_5
2. https://cran.r-project.org/web/packages/EpiEstim/index.html
3. Anne Cori, Neil M. Ferguson, Christophe Fraser, Simon Cauchemez, A New Framework and Software to Estimate Time-Varying Reproduction Numbers During Epidemics, American Journal of Epidemiology, Volume 178, Issue 9, 1 November 2013, Pages 1505–1512, https://doi.org/10.1093/aje/kwt133
4. https://covidtoday.in/
5.  Du, Z., Xu, X., Wu, Y., Wang, L., Cowling, B. J., & Meyers, L. (2020). Serial Interval of COVID-19 among Publicly Reported Confirmed Cases. Emerging Infectious Diseases, 26(6), 1341-1343. https://dx.doi.org/10.3201/eid2606.200357.
