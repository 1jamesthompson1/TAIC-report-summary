from engine.Extract_Analyze.ReportExtracting import ReportExtractor


class TestSafetyIssueExtraction:

    def test_basic_colon(self):
        report_text = """
3.18. Regardless of w hy the system did not activate automatically, there were manual push -
button alarms located on every deck  that would have bypassed the automation and 
activated the fire alarm throughout the ship.  Pushing one of these buttons would have 
been the best method  of raising the alarm for everyone on board.  None of the crew 
pushed a manual button.  
Firefighting procedures and equipment  
Safety issue: Some aspects of the crew response to the fire did not follow industry good practice.  
3.19. Containing and extinguishing a fire quickly and effectively is critical for preserving life 
and property.  Once the initial individual direct attacks on the fire prove unsuccessful, the 
response would normally focus on containing the fire and using all ava ilable resources to 
extinguish it.  
3.20. The crew of the Dong Won 701  had all received approved shore -based firefighting 
training.  They were all required to be current with the vessel's firefighting procedures.  
This would normally have been achieved during th e crew induction on board and fire 
and emergency drills, which according to the operator's (DW New Zealand Limited 's) 
maritime transport operator plan14 were scheduled to happen four times each year.  
"""
        safety_issues = ReportExtractor(report_text, 'test report').extract_safety_issues()
        assert len(safety_issues) == 1
        assert safety_issues[0] == "Some aspects of the crew response to the fire did not follow industry good practice."

    def test_basic_multi_colon(self):
        report_text = """
3.18. Regardless of w hy the system did not activate automatically, there were manual push -
button alarms located on every deck  that would have bypassed the automation and 
activated the fire alarm throughout the ship.  Pushing one of these buttons would have 
been the best method  of raising the alarm for everyone on board.  None of the crew 
pushed a manual button.  
Firefighting procedures and equipment  
Safety issue: Some aspects of the crew response to the fire did not follow industry good practice.  
3.19. Containing and extinguishing a fire quickly and effectively is critical for preserving life 
and property.  Once the initial individual direct attacks on the fire prove unsuccessful, the 
response would normally focus on containing the fire and using all ava ilable resources to 
extinguish it.  
3.20. The crew of the Dong Won 701  had all received approved shore -based firefighting 
training.  They were all required to be current with the vessel's firefighting procedures.  
Safety issue: Inconsistencies in the application of Rule 40D may have resulted in up to 12 fishing 
vessels operating under the New Zealand Flag not complying fully with the relevant safety 
3.21. The crew of the Dong Won 701  had all received approved shore -based firefighting  
"""
        safety_issues = ReportExtractor(report_text, 'test report').extract_safety_issues()
        assert len(safety_issues) == 2
        assert safety_issues == ["Some aspects of the crew response to the fire did not follow industry good practice.",
                                 "Inconsistencies in the application of Rule 40D may have resulted in up to 12 fishing vessels operating under the New Zealand Flag not complying fully with the relevant safety"]
        
    def test_basic_hypen(self):
        report_text = """
4.3.5.  The servic ing staff and the train driver were qualified for their roles and all had experience in 
fitting and checking electrical connections bet ween locomotives .  KiwiRail reviewed its 
locomotive servicing procedures and used this derailment to highlight the need for train 
drivers to include visual examination s of electrical connection s when dealing with  any similar 
locomotive performance issues .  Consequently, t he Commission has not made any 
recommendations to KiwiRail to address this issue.  
4.4. Incorrect brake handle  set-up 
Safety issue - Driver B was able to set the brake handles incorrectly because there was no 
inter lock6 capability between the two driving cabs of the DL-class locomotive s.  The incorrect 
brake set -up resulted in  driver B not having brake control over the coupled wagons . 
4.4.1.  The DL-class  locomotive was  the first double -cab diesel locomotive to operate in New Zealand 
that had brake handles permanently fitted to both cabs.   It required train drivers to set the  
locomotive  and train brake handles correctly before vacating a cab and relocating to the cab 
at the other end.    
"""
        safety_issues = ReportExtractor(report_text, 'test report').extract_safety_issues()
        assert len(safety_issues) == 1
        assert safety_issues[0] == "Driver B was able to set the brake handles incorrectly because there was no interlock capability between the two driving cabs of the DL-class locomotives. The incorrect brake set-up resulted in driver B not having brake control over the coupled wagons."

    def test_basic_multi_hypen(self):
        report_text = """
4.3.5.  The servic ing staff and the train driver were qualified for their roles and all had experience in 
fitting and checking electrical connections bet ween locomotives .  KiwiRail reviewed its 
locomotive servicing procedures and used this derailment to highlight the need for train 
drivers to include visual examination s of electrical connection s when dealing with  any similar 
locomotive performance issues .  Consequently, t he Commission has not made any 
recommendations to KiwiRail to address this issue.  
4.4. Incorrect brake handle  set-up 
Safety issue - Driver B was able to set the brake handles incorrectly because there was no 
inter lock6 capability between the two driving cabs of the DL-class locomotive s.  The incorrect 
brake set -up resulted in  driver B not having brake control over the coupled wagons . 
4.4.1.  The DL-class  locomotive was  the first double -cab diesel locomotive to operate in New Zealand 
that had brake handles permanently fitted to both cabs.   It required train drivers to set the  
locomotive  and train brake handles correctly before vacating a cab and relocating to the cab 
at the other end.  
4.4.2.  In comparison , the e arlier -generation double -cab diesel  and electric locomotives , and the 
current fleet of double -cab EF -class electric locomotives , required some  or all of the brake 
handles  to be physically transferred by the drivers when changing ends.   This meant that train 
drivers had to isolate the brakes in a cab being vacated in order to remove the appropriate 
handles .  The handles were then carried to the other cab , where th ey were inserted to allow 
                                                        
5 An ammeter is a dial fitted on a locomoti ve's dashboard to display the amount of current being produced by 
the electrical generator.  
6 A mechanical or electrical feature that ensures that the operation of two linked mechanisms, brake handles 
in this instance, cannot be independently operated at t he same time . 
 

<< Page 12 >>
 Final Repot RO -2013 -101 the brakes  to be controlled from that end of the locomotive .  This manual process provided a  
reliable  interlock , prevent ing an incorrect brake handle set -up as seen in this instance . 
4.4.3.  KiwiRail investigated several options before choosing the Tranzlog event recorder system to 
provide an interlock capability  to prevent a similar incident .  The Tranzlog system was chosen 
to perform  this interlock b ecause the positions of the brake handles were  already being 
monitored by the system.  This option was successfully t ested during 2014/2015 and rolled 
out on all the DL-class  locomotives.  In view of this work, t he Commission has not made a 
recommendation to KiwiRail to address this issue.  
4.4.4.  Additionally , KiwiRail has committed to provid ing a similar Tranzlog -based interlock capability  
on all its other mainline diesel locomotives .  This work will supplement the existing manual 
interlock and was underway at the time of the compilation of this report.  
Finding s 
1. The train departed Gl enbrook with only the locomotive brakes operating.  The 
brakes on all of the wagons (the train brake) were not working because the driver 
had omitted to configure the brake levers correctly when changing from one 
driving cab to the other.  
2. The driver was ab le to depart with a wrongly configured brake system because 
there was no mechanical interlock to prevent his doing so.  
3. The required pre -departure train brake test would have revealed that the train 
brake was not operational, but those involved omitted to p erform the test when a 
third locomotive was attached to the train.  
4. The train parted when a coupling that had not been fully secured allowed the 
hook to spring up when the couplings suddenly compressed as the train 
descended the gradient from Glenbrook . 
5. The derailment was caused by the rear portion of the train colliding with the 
forward portion seconds after the train parted.  
4.5. Non-technical skills  
Safety issue - When the three staff members came together to couple the third  locomotive to 
the disabled train at Glenbrook, no challenge and confirm action s were taken to complete a 
fundamental brake test procedure , which was designed to ensure  that the trains ' air brakes 
were functioning correctly.  
4.5.1.  Non-technical skills (previously known as crew resource managemen t) are a set of practices 
designed to create a safe working environment, encourage teamwork, improve situational 
awareness and understand technical proficiency . 
4.5.2.  Staff using non-technical skills will communicate more effectively , be more aware of their 
situation, use all of its available resources  and work better with one another.   Communication 
skills and practices form a significant component of what has become known as non -technical 
skills in other transport modes.
"""
        safety_issues = ReportExtractor(report_text, 'test report').extract_safety_issues()
        assert len(safety_issues) == 2
        assert safety_issues == [
            "Driver B was able to set the brake handles incorrectly because there was no interlock capability between the two driving cabs of the DL-class locomotives. The incorrect brake set-up resulted in driver B not having brake control over the coupled wagons.",
            "When the three staff members came together to couple the third locomotive to the disabled train at Glenbrook, no challenge and confirm actions were taken to complete a fundamental brake test procedure, which was designed to ensure that the trains' air brakes were functioning correctly."
        ]